local M = {}

M.create_buffer = function()
  local bf = vim.api.nvim_create_buf(true, true)
  vim.api.nvim_buf_set_name(bf, "*databricks*")
  vim.api.nvim_set_option_value("filetype", "txt", { buf = bf })
  -- print(vim.g.databricks_buf)
  vim.g.databricks_buf = bf
  return bf
end

M.clear_buffer = function(bf)
  vim.api.nvim_buf_set_lines(bf, 0, -1, false, {})
end

M.write_output_to_buffer = function(bf, output_table, start_line)
  local stringified = vim.fn.json_encode(output_table)
  local lines = vim.fn.split(stringified, "\n")
  table.insert(lines, 1, "Output:")
  table.insert(lines, 1, "")
  local end_line = start_line + table.getn(lines)
  vim.api.nvim_buf_set_lines(bf, start_line, end_line, false, lines)
end

M.write_visual_selection_to_buffer = function(bf, lines)
  M.clear_buffer(bf) -- for now: overwrite the buffer with every execution
  vim.api.nvim_buf_set_lines(bf, 0, table.getn(lines), false, lines) -- misalighnment might cause problems here
end

return M
