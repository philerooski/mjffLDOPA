pebble_data = synread("syn17104010")
pebble_data = pebble_data[:1000]
pebble_data.set_index("timestamp", inplace = True)
pebble_data = pebble_data.astype(float)
dummy_pebble_data = pebble_data.apply(lambda c : c + random.randn(len(c)) * 0.1)
dummy_pebble_data.to_csv("dummyPebbleData_Day1.txt", sep = "\t")
f = sc.File("dummyPebbleData_Day1.txt", parent = "syn18085868")
syn.store(f)
