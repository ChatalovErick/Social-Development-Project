[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data]{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fertility_rates_global_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fertility_rates_global = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "World_Bank_Demographic_Statistics_Database.fertility_rates_global", ItemKind = "Table"]}[Data],
  #"Added index" = Table.AddIndexColumn(#"Navigation 2", "Index", 0, 1, Int64.Type),
  #"Renamed columns" = Table.RenameColumns(#"Added index", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns", {"ID"}, false),
  #"Reordered columns" = Table.ReorderColumns(#"Marked key columns", {"ID", "Country_Code", "Year", "Fertility_Rate_Births_Per_Woman"})
in
  #"Reordered columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "population_and_migration_global_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared population_and_migration_global = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "World_Bank_Demographic_Statistics_Database.population_and_migration_global", ItemKind = "Table"]}[Data],
  #"Added index" = Table.AddIndexColumn(#"Navigation 2", "Index", 0, 1, Int64.Type),
  #"Renamed columns" = Table.RenameColumns(#"Added index", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns", {"ID"}, false),
  #"Reordered columns" = Table.ReorderColumns(#"Marked key columns", {"ID", "Country_Code", "Year", "Migrant_Stock_Total_Count", "Pop_Total_Count", "Pop_Female_Count", "Pop_Male_Count"})
in
  #"Reordered columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "electricity_access_and_consumption_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared electricity_access_and_consumption = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "World_Bank_Development_Statistics_Database.electricity_access_and_consumption", ItemKind = "Table"]}[Data],
  #"Added index" = Table.AddIndexColumn(#"Navigation 2", "Index", 0, 1, Int64.Type),
  #"Renamed columns" = Table.RenameColumns(#"Added index", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns", {"ID"}, false),
  #"Reordered columns" = Table.ReorderColumns(#"Marked key columns", {"ID", "Country_Code", "Year", "Access_Rural_Pct", "Access_Urban_Pct", "Access_Total_Pct", "KWh_Per_Capita"})
in
  #"Reordered columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "country_economic_indicators_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared country_economic_indicators = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "World_Bank_Economic_Statistics_Database.country_economic_indicators", ItemKind = "Table"]}[Data],
  #"Added index" = Table.AddIndexColumn(#"Navigation 2", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Country", "Year", "Inflation_CPI_%", "GDP_USD", "GDP_Growth_Annual_%", "GDP_PPP", "GDP_Per_Capita"}),
  #"Marked key columns" = Table.AddKey(#"Reordered columns", {"Index"}, false),
  #"Renamed columns" = Table.RenameColumns(#"Marked key columns", {{"Index", "ID"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "YR", "", Replacer.ReplaceText, {"Year"})
in
  #"Replaced value";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "educational_attainment_global_pct_by_se_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared educational_attainment_global_pct_by_se = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "World_Bank_Education_Statistics_Database.educational_attainment_global_pct_by_sex", ItemKind = "Table"]}[Data],
  #"Added index" = Table.AddIndexColumn(#"Navigation 2", "Index", 0, 1, Int64.Type),
  #"Renamed columns" = Table.RenameColumns(#"Added index", {{"Index", "ID"}}),
  #"Reordered columns" = Table.ReorderColumns(#"Renamed columns", {"ID", "Country", "Year", "Bachelor_Female_Pct", "Bachelor_Male_Pct", "Doctoral_Female_Pct", "Doctoral_Male_Pct", "Master_Male_Pct", "Master_Total_Pct", "Short_Cycle_Female_Pct", "Short_Cycle_Male_Pct"}),
  #"Marked key columns" = Table.AddKey(#"Reordered columns", {"ID"}, false),
  #"Replaced value" = Table.ReplaceValue(#"Marked key columns", "YR", "", Replacer.ReplaceText, {"Year"})
in
  #"Replaced value";
shared fertility_rates_global_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "fertility_rates_global", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared population_and_migration_global_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "population_and_migration_global", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared electricity_access_and_consumption_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "electricity_access_and_consumption", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared country_economic_indicators_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "country_economic_indicators", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared educational_attainment_global_pct_by_se_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "educational_attainment_global_pct_by_se", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
