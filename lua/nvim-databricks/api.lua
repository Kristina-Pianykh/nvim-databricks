local curl = require("plenary.curl")
local StringUtils = require("databricks.strings")
local BufferUtils = require("databricks.buffer")
local CURR_SCRIPT_DIR = vim.fn.expand(vim.fn.expand("%:p:h"))

local M = {}

M.CONTEXT_STATUS = {
  running = "Running",
  pending = "Pending",
  error = "Error",
}

M.COMMAND_STATUS = {
  running = "Running",
  cancelled = "Cancelled",
  cancelling = "Cancelling",
  finished = "Finished",
  queued = "Queued",
  error = "Error",
}

function M:get_command_status(command_id)
  local url = "https://" .. self.creds.host .. "/api/1.2/commands/status"
  local header = {
    Authorization = "Bearer " .. self.creds.token,
    accept = "application/json",
  }
  local query = {
    clusterId = self.config.settings.cluster_id,
    contextId = self.context_id,
    commandId = command_id,
  }

  local args = {
    headers = header,
    query = query,
  }
  print(vim.inspect({ url, args }))

  local response = curl.get(url, args)
  local response_body = vim.fn.json_decode(response.body)
  print(vim.inspect(response_body))
  if response.status == 200 then
    return response_body
  else
    error("Failed to get status of the command with id: " .. command_id)
  end
end

function M:wait_command_status_until_finished_or_error(command_id)
  local now = os.time()
  local timeout = 60 * 20 -- 20 seconds TODO: make configurable
  local deadline = now + timeout
  local target_states = { M.COMMAND_STATUS.finished, M.COMMAND_STATUS.error }
  local failed_states =
    { M.COMMAND_STATUS.cancelled, M.COMMAND_STATUS.cancelling }

  local attempt = 1
  -- local sleep = attempt * 1000 -- in millies
  local sleep = attempt

  while os.time() < deadline do
    local ok, res = pcall(M.get_command_status, self, command_id)

    if not ok then
      error(res)
    end

    local response_body = res
    local status = response_body.status

    if StringUtils.contains(target_states, status) then
      print("Execution reached target state: " .. status)
      return response_body
    elseif StringUtils.contains(failed_states, status) then
      error("failed to reach Finished or Error, got " .. status)
    else
      os.execute("sleep " .. tonumber(sleep)) -- TODO: replace when wrapping into async
    end

    attempt = attempt + 1
    -- if sleep < 10 * 1000 then
    if sleep < 10 then -- sleep no longer than 10s
      sleep = attempt
    end
  end
  error("Timed out after " .. timeout .. "s.")
end

function M:execute_code(command)
  local url = "https://" .. self.creds.host .. "/api/1.2/commands/execute"
  local header = {
    Authorization = "Bearer " .. self.creds.token,
    accept = "application/json",
    content_type = "application/json",
  }
  local data = {
    clusterId = self.config.settings.cluster_id,
    language = "python",
    contextId = self.context_id,
    command = command,
  }

  local args = {
    headers = header,
    body = vim.fn.json_encode(data),
  }
  print(vim.inspect({ url, args }))

  local response = curl.post(url, args)
  local response_body = vim.fn.json_decode(response.body)
  print(vim.inspect(response_body))

  if response.status ~= 200 then
    error(
      "request failed with status "
        .. response.status
        .. ". Error: "
        .. response_body.error
    )
  end
  local command_id = response_body.id
  local ok, res =
    pcall(M.wait_command_status_until_finished_or_error, self, command_id)

  if not ok then
    error(res)
  else
    return res.results
  end
end

function M:clear_context()
  local path = CURR_SCRIPT_DIR .. "/.execution_context"
  -- local context_id = nil

  local f = io.open(path, "r")
  if f == nil then
    return
  else
    self.context_id = f:read("*all")
    assert(self.context_id)
    f:close()
  end

  if self.context_id ~= nil then
    local url = "https://" .. self.creds.host .. "/api/1.2/contexts/destroy"
    local header = {
      Authorization = "Bearer " .. self.creds.token,
      accept = "application/json",
      content_type = "application/json",
    }
    local data = {
      clusterId = self.config.settings.cluster_id,
      contextId = self.context_id,
    }

    local args = {
      headers = header,
      body = vim.fn.json_encode(data),
    }
    print(vim.inspect({ url, args }))

    local response = curl.post(url, args)
    print(vim.inspect(response))
    local response_body = vim.fn.json_decode(response.body)
    print(vim.inspect(response_body))
  end

  os.remove(path)
  self.context_id = nil
end

function M.create_execution_context(creds, cluster_id)
  local url = "https://" .. creds.host .. "/api/1.2/contexts/create"
  local header = {
    Authorization = "Bearer " .. creds.token,
    accept = "application/json",
    content_type = "application/json",
  }
  local data = { clusterId = cluster_id, language = "python" }

  local args = {
    headers = header,
    body = vim.fn.json_encode(data),
  }
  print(vim.inspect({ url, args }))

  local response = curl.post(url, args)
  local response_body = vim.fn.json_decode(response.body)
  print(vim.inspect(response_body))

  if response.status ~= 200 then
    error(
      "request failed with status "
        .. response.status
        .. ". Error: "
        .. response_body.error
    )
  end

  local context_id = response_body.id
  local f = assert(io.open(CURR_SCRIPT_DIR .. "/.execution_context", "w"))
  f:write(context_id)
  f:close()

  print(context_id)
  return context_id
end

function M:get_context_status()
  -- print(vim.inspect(self.creds))
  -- print(vim.inspect(self.config))
  -- print(vim.inspect(self))
  local url = "https://"
    .. self.creds.host
    .. "/api/1.2/contexts/status"
    .. "?clusterId="
    .. self.config.settings.cluster_id
    .. "&contextId="
    .. self.context_id
  local header = {
    Authorization = "Bearer " .. self.creds.token,
    accept = "application/json",
    content_type = "application/json",
  }

  local args = {
    headers = header,
  }

  print(vim.inspect({ url, args }))
  local response = curl.get(url, args)
  print(vim.inspect(response))
  local response_body = vim.fn.json_decode(response.body)
  print(vim.inspect(response_body))

  if response.status ~= 200 then
    error(
      "Failed to get the status of the execution context. Error: "
        .. response_body.error
    )
  end

  return response_body.status
end

function M:write_cmd_to_buffer()
  local lines = StringUtils.get_visual_selection()
  assert(lines)
  local command = table.concat(lines, "\n")

  BufferUtils.write_visual_selection_to_buffer(self.buf, lines)

  local ok, res = pcall(M.execute_code, self, command)

  if not ok then
    error(res)
  else
    assert(type(res) == "table")

    if res.data ~= nil then
      print("Output: " .. res.data)
    end

    BufferUtils.write_output_to_buffer(self.buf, res, table.getn(lines))
    return res
  end
end

function M:create_context_if_not_exists()
  local context_status = nil
  local ok, res

  local f = io.open(CURR_SCRIPT_DIR .. "/.execution_context", "r")
  if f == nil then
    ok, res = pcall(
      M.create_execution_context,
      self.creds,
      self.config.settings.cluster_id
    )

    if ok then
      self.context_id = res
      context_status = M.CONTEXT_STATUS.running
    else
      error(res)
    end
  else
    self.context_id = f:read("*all")
    f:close()
    assert(self.context_id)
    ok, res = pcall(self.get_context_status, self)

    if ok then
      context_status = res
    else -- context is outdates or invalid. Recreate
      self:clear_context()
      ok, res = pcall(
        M.create_execution_context,
        self.creds,
        self.config.settings.cluster_id
      )

      if ok then
        self.context_id = res
        context_status = M.CONTEXT_STATUS.running
      else
        error(res)
      end
    end

    assert(context_status)

    if context_status ~= M.CONTEXT_STATUS.running then
      if context_status == M.CONTEXT_STATUS.pending then
        error("Execution context's status is pending. Try later...")
      else
        self:clear_context()
        ok, res = pcall(
          M.create_execution_context,
          self.creds,
          self.config.settings.cluster_id
        )

        if ok then
          self.context_id = res
          context_status = M.CONTEXT_STATUS.running
        else
          error(res)
        end
      end
    end
  end
  print(context_status)
  if not assert(self.context_id) then
    error("Failed to create execution context.")
  end
end

function M:launch()
  self:create_context_if_not_exists()

  local lines = StringUtils.get_visual_selection()
  assert(lines)
  local command = table.concat(lines, "\n")

  BufferUtils.write_visual_selection_to_buffer(vim.g.databricks_buf, lines)

  local ok, res = pcall(M.execute_code, self, command)

  if not ok then
    error(res)
  else
    assert(type(res) == "table")

    if res.data ~= nil then
      print("Output: " .. res.data)
    end

    BufferUtils.write_output_to_buffer(
      vim.g.databricks_buf,
      res,
      table.getn(lines)
    )
    return res
  end
end

return M
