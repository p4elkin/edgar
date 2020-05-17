from xbrl import XBRLParser, GAAPSerializer, DEISerializer

xbrl_parser = XBRLParser()
xbrl = xbrl_parser.parse(open("../data/xbrl/apple_inc/2013/qtr1/apple_inc-10-Q-QTR1-2013-01-240001193125-13-022339-xbrl/aapl-20121229.xml"))

gaap_obj = xbrl_parser.parseGAAP(xbrl, doc_date="20121229", context="current", ignore_errors=0)
print("Cash generated by operating activities: " + str(gaap_obj.net_cash_flows_operating))
print("Cash generated by/(used in) investing activities: " + str(gaap_obj.net_cash_flows_investing))
print("Cash used in financing activities: " + str(gaap_obj.net_cash_flows_financing))
print("Total shareholders’ equity: " + str(gaap_obj.stockholders_equity))
print("Total assets: " + str(gaap_obj.assets))
print("Operating income: " + str(gaap_obj.operating_income_loss))
print("Net income: " + str(gaap_obj.net_income_loss))
print("Total net sales: " + str(gaap_obj.revenues))

dei_obj = xbrl_parser.parseDEI(xbrl)
# Serialize the DEI data
serializer = DEISerializer()
result = serializer.dump(dei_obj)

# serializer = GAAPSerializer()
# result = serializer.dump(gaap_obj)
#
# print(result.data)
