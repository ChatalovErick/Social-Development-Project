[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data]{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "pop_age_sex_urban_rural_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared pop_age_sex_urban_rural = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "UN_Census_Demographic_Statistics_Database.pop_age_sex_urban_rural", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Table_Code", "Country_or_Area", "Reference_Date", "Area", "Sex", "Value_Footnotes", "Source_Year", "Reliability"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each not Text.Contains([Age], "-")),
  #"Removed columns 1" = Table.RemoveColumns(#"Filtered rows", {"Record_Type", "Age", "Record_Type_Code", "Reliability_Code"}),
  #"Added index" = Table.AddIndexColumn(#"Removed columns 1", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Country_or_Area_Code", "Year", "Area_Code", "Sex_Code", "Code_of_Age", "Value"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns", {"ID"}, false)
in
  #"Marked key columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "employed_pop_by_status_in_employment_by_age_area_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared employed_pop_by_status_in_employment_by_age_area = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "UN_Census_Economic_Statistics_Database.employed_population_by_status_in_employment_age_and_sex", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Table_Code", "Country_or_Area", "Reference_Date", "Area", "Sex", "Status_in_employment", "Record_Type_Code", "Record_Type", "Reliability", "Reliability_Code", "Source_Year", "Value_Footnotes"}),
  #"Added index" = Table.AddIndexColumn(#"Removed columns", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Country_or_Area_Code", "Year", "Area_Code", "Sex_Code", "Age", "Code_of_Status_in_employment", "Value"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns", {"ID"}, false)
in
  #"Marked key columns";
[BindToDefaultDestination = true]
shared gini_index = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "UN_Census_Economic_Statistics_Database.gini_index", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Indicator_Code", "Value_Footnotes", "Country_or_Area", "Time_Code"}),
  #"Added index" = Table.AddIndexColumn(#"Removed columns", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Country_or_Area_Code", "Year", "Value"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"Index", "Gini_ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns", {"Gini_ID"}, false)
in
  #"Marked key columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "unemployment_rate_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared unemployment_rate = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "UN_Census_Economic_Statistics_Database.unemployment_rate", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Country_or_Area", "Source", "Unit", "Value_Footnotes"}),
  #"Added index" = Table.AddIndexColumn(#"Removed columns", "Index", 0, 1, Int64.Type),
  #"Renamed columns" = Table.RenameColumns(#"Added index", {{"Index", "ID"}}),
  #"Reordered columns" = Table.ReorderColumns(#"Renamed columns", {"ID", "Country_or_Area_Code", "Subgroup", "Year", "Value"}),
  #"Marked key columns" = Table.AddKey(#"Reordered columns", {"ID"}, false)
in
  #"Marked key columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "pop_attainment_demographics_15plus_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared pop_attainment_demographics_15plus = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "UN_Census_Education_Statistics_Database.pop_attainment_demographics_15plus", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Table_Code", "Country_or_Area", "Reference_Date", "Area", "Sex", "Code_of_Age", "Record_Type_Code", "Record_Type", "Reliability_Code", "Reliability", "Value_Footnotes", "Source_Year", "Educational_attainment"}),
  #"Added index" = Table.AddIndexColumn(#"Removed columns", "Index", 0, 1, Int64.Type),
  #"Renamed columns" = Table.RenameColumns(#"Added index", {{"Index", "ID"}}),
  #"Reordered columns" = Table.ReorderColumns(#"Renamed columns", {"ID", "Country_or_Area_Code", "Year", "Area_Code", "Sex_Code", "Age", "Code_of_Educational_attainment", "Value"})
in
  #"Reordered columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "school_attendance_urban_rural_5to24_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared school_attendance_urban_rural_5to24 = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "UN_Census_Education_Statistics_Database.school_attendance_urban_rural_5to24", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 2", {"Table_Code", "Country_or_Area", "Reference_Date", "Area", "Sex", "Code_of_Age", "Record_Type_Code", "Record_Type", "Reliability_Code", "Reliability", "Source_Year", "Value_Footnotes", "School_attendance"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns", each not Text.Contains([Age], "-")),
  #"Added index" = Table.AddIndexColumn(#"Filtered rows", "Index", 0, 1, Int64.Type),
  #"Renamed columns" = Table.RenameColumns(#"Added index", {{"Index", "ID"}}),
  #"Marked key columns" = Table.AddKey(#"Renamed columns", {"ID"}, false),
  #"Reordered columns" = Table.ReorderColumns(#"Marked key columns", {"ID", "Country_or_Area_Code", "Year", "Area_Code", "Sex_Code", "Age", "Code_of_School_attendance", "Value"})
in
  #"Reordered columns";
shared pop_age_sex_urban_rural_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "pop_age_sex_urban_rural", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared employed_pop_by_status_in_employment_by_age_area_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "employed_pop_by_status_in_employment_by_age_area", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared unemployment_rate_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "unemployment_rate", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared pop_attainment_demographics_15plus_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "pop_attainment_demographics_15plus", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared school_attendance_urban_rural_5to24_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "school_attendance_urban_rural_5to24", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
