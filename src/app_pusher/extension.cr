struct JSON::Any
  def is_true? : Bool
    case v = @raw
    when Int
      return v != 0
    when String
      return v.downcase == "true"
    when Bool
      return v
    else
      return false
    end
  end
end
