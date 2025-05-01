describe "database" do
  def run_script(commands)
    raw_output = nil
    IO.popen("./build/opendb", "r+") do |pipe|
      commands.each do |command|
        pipe.puts command
      end

      pipe.close_write

      # Read entire output
      raw_output = pipe.gets(nil)
    end
    raw_output.split("\n")
  end

  it "INSERTS and RETRIEVES a row" do
    result = run_script([
        "INSERT 1 cstack cstack@example.com",
        "SELECT",
        ".exit"
    ])
    expect(result).to match_array([
        "opendb > Executed.",
        "opendb > (1, cstack, cstack@example.com)",
        "Executed.",
        "opendb > ",
    ])
  end

  it "prints an error message when table is full" do
    script = (1..1401).map do |i|
        "INSERT #{i} user#{i} user#{i}@example.com"
    end
    script << ".exit"
    result = run_script(script)
    expect(result[-2]).to eq("opendb > Error: Table full.")
  end

  it 'allows inserting strings that are the maximum length' do
    long_username = "a"*32
    long_email = "a"*255
    script = [
      "INSERT 1 #{long_username} #{long_email}",
      "SELECT",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "opendb > Executed.",
      "opendb > (1, #{long_username}, #{long_email})",
      "Executed.",
      "opendb > ",
    ])
  end
  
  it "prints an error message if string are too long" do
    long_username = "a"*33
    long_email = "a"*256
    script = [
      "INSERT 1 #{long_username} #{long_email}",
      "SELECT",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "opendb > String is too long.",
      "Executed.",
      "opendb > ",
    ])
  end

  it "prints an error message if id is negative" do
    script = [
      "INSERT -1 someone someone@example.com",
      "SELECT",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "opendb > ID must be positive.",
      "Executed.",
      "opendb > ",
    ])
  end

end
