local M = {}

M.contains = function(arr, val)
  for _, v in ipairs(arr) do
    if v == val then
      return true
    end
  end
  return false
end

-- function copied from https://www.reddit.com/r/neovim/comments/1b1sv3a/function_to_get_visually_selected_text/
M.get_visual_selection = function()
  local _, srow, scol = unpack(vim.fn.getpos("v"))
  local _, erow, ecol = unpack(vim.fn.getpos("."))

  -- visual line mode
  if vim.fn.mode() == "V" then
    if srow > erow then
      return vim.api.nvim_buf_get_lines(0, erow - 1, srow, true)
    else
      return vim.api.nvim_buf_get_lines(0, srow - 1, erow, true)
    end
  end

  -- visual block mode
  if vim.fn.mode() == "\22" then
    local lines = {}
    if srow > erow then
      srow, erow = erow, srow
    end
    if scol > ecol then
      scol, ecol = ecol, scol
    end
    for i = srow, erow do
      table.insert(
        lines,
        vim.api.nvim_buf_get_text(
          0,
          i - 1,
          math.min(scol - 1, ecol),
          i - 1,
          math.max(scol - 1, ecol),
          {}
        )[1]
      )
    end
    return lines
  end
end

M.is_empty = function(arg)
  return arg == nil or arg == ""
end

-- function string.starts(String, Start)
--   return string.sub(String, 1, string.len(Start)) == Start
-- end

M.string_starts = function(String, Start)
  return string.sub(String, 1, string.len(Start)) == Start
end

-- M.string.starts = function(String, Start)
--   return string.sub(String, 1, string.len(Start)) == Start
-- end

return M
