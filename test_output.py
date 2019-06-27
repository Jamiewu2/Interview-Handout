with open('./output.txt', 'r') as file:
    contents = file.read()
    lines = contents.split('\n\n')
    for line in lines:
        assert len(line) == 0 or line.strip() == 'Maestro is the best......'