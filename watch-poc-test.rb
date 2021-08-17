require 'kubeclient'
require 'json'

def write_to_file(podInventory)
    puts "in_kube_podinventory:: write_to_file : inside write to file function"
    File.write("testing-podinventory.json", JSON.pretty_generate(podInventory))
    puts "in_kube_podinventory:: write_to_file : successfully done writing to file"
end

def getPodInventoryRecords(item)
    records = []
    record = {}

    batchTime = Time.now.utc.iso8601

    begin
      record["CollectionTime"] = batchTime #This is the time that is mapped to become TimeGenerated
      record["Name"] = item["metadata"]["name"]
      podNameSpace = item["metadata"]["namespace"]
      podUid = KubernetesApiClient.getPodUid(podNameSpace, item["metadata"])
      if podUid.nil?
        return records
      end

      nodeName = ""
      #for unscheduled (non-started) pods nodeName does NOT exist
      if !item["spec"]["nodeName"].nil?
        nodeName = item["spec"]["nodeName"]
      end
      # For ARO v3 cluster, skip the pods scheduled on to master or infra nodes
      if KubernetesApiClient.isAROv3MasterOrInfraPod(nodeName)
        return records
      end

      record["PodUid"] = podUid
      record["PodLabel"] = [item["metadata"]["labels"]]
      record["Namespace"] = podNameSpace
      record["PodCreationTimeStamp"] = item["metadata"]["creationTimestamp"]
      #for unscheduled (non-started) pods startTime does NOT exist
      if !item["status"]["startTime"].nil?
        record["PodStartTime"] = item["status"]["startTime"]
      else
        record["PodStartTime"] = ""
      end
      #podStatus
      # the below is for accounting 'NodeLost' scenario, where-in the pod(s) in the lost node is still being reported as running
      podReadyCondition = true
      if !item["status"]["reason"].nil? && item["status"]["reason"] == "NodeLost" && !item["status"]["conditions"].nil?
        item["status"]["conditions"].each do |condition|
          if condition["type"] == "Ready" && condition["status"] == "False"
            podReadyCondition = false
            break
          end
        end
      end
      if podReadyCondition == false
        record["PodStatus"] = "Unknown"
        # ICM - https://portal.microsofticm.com/imp/v3/incidents/details/187091803/home
      elsif !item["metadata"]["deletionTimestamp"].nil? && !item["metadata"]["deletionTimestamp"].empty?
        record["PodStatus"] = Constants::POD_STATUS_TERMINATING
      else
        record["PodStatus"] = item["status"]["phase"]
      end
      #for unscheduled (non-started) pods podIP does NOT exist
      if !item["status"]["podIP"].nil?
        record["PodIp"] = item["status"]["podIP"]
      else
        record["PodIp"] = ""
      end

      record["Computer"] = nodeName
      record["ClusterId"] = KubernetesApiClient.getClusterId
      record["ClusterName"] = KubernetesApiClient.getClusterName
      record["ServiceName"] = getServiceNameFromLabels(item["metadata"]["namespace"], item["metadata"]["labels"], serviceRecords)

      if !item["metadata"]["ownerReferences"].nil?
        record["ControllerKind"] = item["metadata"]["ownerReferences"][0]["kind"]
        record["ControllerName"] = item["metadata"]["ownerReferences"][0]["name"]
        @controllerSet.add(record["ControllerKind"] + record["ControllerName"])
        #Adding controller kind to telemetry ro information about customer workload
        if (@controllerData[record["ControllerKind"]].nil?)
          @controllerData[record["ControllerKind"]] = 1
        else
          controllerValue = @controllerData[record["ControllerKind"]]
          @controllerData[record["ControllerKind"]] += 1
        end
      end
      podRestartCount = 0
      record["PodRestartCount"] = 0

      #Invoke the helper method to compute ready/not ready mdm metric
      @inventoryToMdmConvertor.process_record_for_pods_ready_metric(record["ControllerName"], record["Namespace"], item["status"]["conditions"])

      podContainers = []
      if item["status"].key?("containerStatuses") && !item["status"]["containerStatuses"].empty?
        podContainers = podContainers + item["status"]["containerStatuses"]
      end
      # Adding init containers to the record list as well.
      if item["status"].key?("initContainerStatuses") && !item["status"]["initContainerStatuses"].empty?
        podContainers = podContainers + item["status"]["initContainerStatuses"]
      end
      # if items["status"].key?("containerStatuses") && !items["status"]["containerStatuses"].empty? #container status block start
      if !podContainers.empty? #container status block start
        podContainers.each do |container|
          containerRestartCount = 0
          lastFinishedTime = nil
          # Need this flag to determine if we need to process container data for mdm metrics like oomkilled and container restart
          #container Id is of the form
          #docker://dfd9da983f1fd27432fb2c1fe3049c0a1d25b1c697b2dc1a530c986e58b16527
          if !container["containerID"].nil?
            record["ContainerID"] = container["containerID"].split("//")[1]
          else
            # for containers that have image issues (like invalid image/tag etc..) this will be empty. do not make it all 0
            record["ContainerID"] = ""
          end
          #keeping this as <PodUid/container_name> which is same as InstanceName in perf table
          if podUid.nil? || container["name"].nil?
            next
          else
            record["ContainerName"] = podUid + "/" + container["name"]
          end
          #Pod restart count is a sumtotal of restart counts of individual containers
          #within the pod. The restart count of a container is maintained by kubernetes
          #itself in the form of a container label.
          containerRestartCount = container["restartCount"]
          record["ContainerRestartCount"] = containerRestartCount

          containerStatus = container["state"]
          record["ContainerStatusReason"] = ""
          # state is of the following form , so just picking up the first key name
          # "state": {
          #   "waiting": {
          #     "reason": "CrashLoopBackOff",
          #      "message": "Back-off 5m0s restarting failed container=metrics-server pod=metrics-server-2011498749-3g453_kube-system(5953be5f-fcae-11e7-a356-000d3ae0e432)"
          #   }
          # },
          # the below is for accounting 'NodeLost' scenario, where-in the containers in the lost node/pod(s) is still being reported as running
          if podReadyCondition == false
            record["ContainerStatus"] = "Unknown"
          else
            record["ContainerStatus"] = containerStatus.keys[0]
          end
          #TODO : Remove ContainerCreationTimeStamp from here since we are sending it as a metric
          #Picking up both container and node start time from cAdvisor to be consistent
          if containerStatus.keys[0] == "running"
            record["ContainerCreationTimeStamp"] = container["state"]["running"]["startedAt"]
          else
            if !containerStatus[containerStatus.keys[0]]["reason"].nil? && !containerStatus[containerStatus.keys[0]]["reason"].empty?
              record["ContainerStatusReason"] = containerStatus[containerStatus.keys[0]]["reason"]
            end
            # Process the record to see if job was completed 6 hours ago. If so, send metric to mdm
            if !record["ControllerKind"].nil? && record["ControllerKind"].downcase == Constants::CONTROLLER_KIND_JOB
              @inventoryToMdmConvertor.process_record_for_terminated_job_metric(record["ControllerName"], record["Namespace"], containerStatus)
            end
          end

          # Record the last state of the container. This may have information on why a container was killed.
          begin
            if !container["lastState"].nil? && container["lastState"].keys.length == 1
              lastStateName = container["lastState"].keys[0]
              lastStateObject = container["lastState"][lastStateName]
              if !lastStateObject.is_a?(Hash)
                raise "expected a hash object. This could signify a bug or a kubernetes API change"
              end

              if lastStateObject.key?("reason") && lastStateObject.key?("startedAt") && lastStateObject.key?("finishedAt")
                newRecord = Hash.new
                newRecord["lastState"] = lastStateName  # get the name of the last state (ex: terminated)
                lastStateReason = lastStateObject["reason"]
                # newRecord["reason"] = lastStateObject["reason"]  # (ex: OOMKilled)
                newRecord["reason"] = lastStateReason  # (ex: OOMKilled)
                newRecord["startedAt"] = lastStateObject["startedAt"]  # (ex: 2019-07-02T14:58:51Z)
                lastFinishedTime = lastStateObject["finishedAt"]
                newRecord["finishedAt"] = lastFinishedTime  # (ex: 2019-07-02T14:58:52Z)

                # only write to the output field if everything previously ran without error
                record["ContainerLastStatus"] = newRecord

                #Populate mdm metric for OOMKilled container count if lastStateReason is OOMKilled
                if lastStateReason.downcase == Constants::REASON_OOM_KILLED
                  @inventoryToMdmConvertor.process_record_for_oom_killed_metric(record["ControllerName"], record["Namespace"], lastFinishedTime)
                end
                lastStateReason = nil
              else
                record["ContainerLastStatus"] = Hash.new
              end
            else
              record["ContainerLastStatus"] = Hash.new
            end

            #Populate mdm metric for container restart count if greater than 0
            if (!containerRestartCount.nil? && (containerRestartCount.is_a? Integer) && containerRestartCount > 0)
              @inventoryToMdmConvertor.process_record_for_container_restarts_metric(record["ControllerName"], record["Namespace"], lastFinishedTime)
            end
          rescue => errorStr
            $log.warn "Failed in parse_and_emit_record pod inventory while processing ContainerLastStatus: #{errorStr}"
            $log.debug_backtrace(errorStr.backtrace)
            ApplicationInsightsUtility.sendExceptionTelemetry(errorStr)
            record["ContainerLastStatus"] = Hash.new
          end

          podRestartCount += containerRestartCount
          records.push(record.dup)
        end
      else # for unscheduled pods there are no status.containerStatuses, in this case we still want the pod
        records.push(record)
      end  #container status block end

      records.each do |record|
        if !record.nil?
          record["PodRestartCount"] = podRestartCount
        end
      end
    rescue => error
      $log.warn("getPodInventoryRecords failed: #{error}")
    end
    return records
  end

def enumerate
    @podsAPIE2ELatencyMs = 0
    podsAPIChunkStartTime = (Time.now.to_f * 1000).to_i

    puts "in_kube_podinventory::enumerate : Getting pods from Kube API @ #{Time.now.utc.iso8601}"

    podInventory = @KubernetesWatchClient.get_pods(as: :parsed)
    @collection_version = podInventory['metadata']['resourceVersion']

    puts "in_kube_podinventory::enumerate : received collection version: #{@collection_version}"
    puts "in_kube_podinventory::enumerate : Done getting pods from Kube API @ #{Time.now.utc.iso8601}"
    podsAPIChunkEndTime = (Time.now.to_f * 1000).to_i
    @podsAPIE2ELatencyMs = (podsAPIChunkEndTime - podsAPIChunkStartTime)

    if (!podInventory.nil? && !podInventory.empty? && podInventory.key?("items") && !podInventory["items"].nil? && !podInventory["items"].empty?)
        puts "in_kube_podinventory::enumerate : number of pod items :#{podInventory["items"].length}  from Kube API @ #{Time.now.utc.iso8601}"
        puts "in_kube_podinventory::enumerate : time to write to a file and emit to backend - functionality later"
        write_to_file(podInventory)
    else
        puts "in_kube_podinventory::enumerate : Received empty podInventory"
    end
end

def watch
    loop do
        puts "in_kube_pod_inventory::watch: inside infinite loop for watch pods, calling infinite loop"
        enumerate
        puts "in_kube_pod_inventory::watch: inside infinite loop for watch pods, collection version: #{@collection_version}"
        begin
            @KubernetesWatchClient.watch_pods(resource_version: @collection_version, as: :parsed) do |notice|
                puts "in_kube_podinventory::watch : inside watch pods! Time: #{Time.now.utc.iso8601}. Collection version: #{@collection_version}"
                # puts "watch:: notice looks like: #{JSON.pretty_generate(notice)}"
                if !notice.nil? && !notice.empty?
                    puts "in_kube_podinventory::watch : received a notice that is not null and not empty, type: #{notice["type"]}, uid: #{item["metadata"]["uid"]}"

                    item = notice["object"]
                    record = {"name" => item["metadata"]["name"], "uid" => item["metadata"]["uid"], "status" => item["status"]["phase"], "type" => notice["type"]}

                    @mutex.synchronize {
                        @noticeHash[item["metadata"]["uid"]] = record
                    }

                    puts "watch pods:: number of items in noticeHash = #{@noticeHash.size}"
                end
            end
        rescue => exception
            puts "in_kube_podinventory::watch : watch events session got broken and re-establishing the session."
            puts "watch events session broken backtrace: #{exception.backtrace}"
        end
        sleep 300
    end
end

def merge_info
    begin
        fileContents = File.read("testing-podinventory.json")
        puts "in_kube_podinventory::merge_info : file contents read"
        @podHash = JSON.parse(fileContents)
        puts "in_kube_podinventory::merge_info : parse successful"
    rescue => error
        puts "in_kube_podinventory::merge_info : something went wrong with reading file"
        puts "in_kube_podinventory::merge_info : backtrace: #{error.backtrace}"
    end

    puts "in_kube_podinventory::merge_info : before noticeHash loop, number of items in hash: #{@noticeHash.size()}, noticeHash: #{@noticeHash}"

    uidList = []

    @mutex.synchronize {
        @noticeHash.each do |uid, record|
            puts "in_kube_podinventory::merge_info : looping through noticeHash, type of notice: #{record["type"]}"
            # puts "podHash looks like: #{@podHash}"
            puts "notice uid: #{uid}"
            puts "notice record: #{record}"

            uidList.append(uid)

            case record["type"]
            when "ADDED"
                @podHash[uid] = record
                puts "added"
            when "MODIFIED"
                puts "entered modified case as expected"
                if @podHash[uid].nil?
                    @podHash[uid] = record
                else
                    val = @podHash[uid]
                    # puts "checkpoint 1"
                    # puts "val is #{val}"
                    # puts "record status is #{record["status"]}"
                    # puts "old val status is #{val["status"]}"

                    val["status"] = record["status"]
                    # puts "checkpoint 2"
                    @podHash[uid] = val
                end
                puts "modified"
            when "DELETED"
                @podHash.delete(uid)
                puts "deleted"
            else
                puts "something went wrong"
            end
            # puts "uid: #{uid} and record: #{record}"
            puts "end of switch"
        end

        # remove all looked at uids from the noticeHash
        uidList.each do |uid|
            @noticeHash.delete(uid)
        end
    }

    # replace entire contents of testing-podinventory.json
    File.open("testing-podinventory.json", "w") do |f|
        f.write JSON.pretty_generate(@podHash)
    end
end

def run_periodic
    @mutex.lock
    done = @finished
    @nextTimeToRun = Time.now
    @waitTimeout = @run_interval
    until done
        @nextTimeToRun = @nextTimeToRun + @run_interval
        @now = Time.now
        if @nextTimeToRun <= @now
            @waitTimeout = 1
            @nextTimeToRun = @now
        else
            @waitTimeout = @nextTimeToRun - @now
        end
        @condition.wait(@mutex, @waitTimeout)
        done = @finished
        @mutex.unlock
        if !done
            begin
                puts "in_kube_podinventory::run_periodic : about to call merge. start time: #{Time.now.utc.iso8601}"
                merge_info
                puts "in_kube_podinventory::run_periodic : finished calling merge. end time: #{Time.now.utc.iso8601}"
            rescue => errorStr
                puts "in_kube_podinventory::run_periodic : Failed to retrieve pod inventory: #{errorStr}"
            end
        end
        @mutex.lock
    end
    @mutex.unlock
end

def start
    @noticeHash = {}
    @finished = false
    @mutex = Mutex.new
    @condition = ConditionVariable.new
    @run_interval = 60

    @KubernetesWatchClient = Kubeclient::Client.new('http://localhost:8080/api', 'v1', as: :parsed)

    puts "about to call enumerate"
    enumerate

    # puts "mmap creation"
    # mmap = Mmap.new("mmapfile.map")

    watchthread = Thread.new{watch()}
    runthread = Thread.new{run_periodic()}
    watchthread.join
    runthread.join    
end


start