import os 

cur_path = os.getcwd()
print(cur_path)

test_path = "C:/scb100690/UR/GEN_DPL"
print(test_path.replace("/", "\\"))

# os.system(f'explorer {cur_path}')