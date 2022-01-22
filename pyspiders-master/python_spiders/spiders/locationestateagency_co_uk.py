# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'locationestateagency_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Locationestateagency_Co_PySpider_united_kingdom"
    post_urls = ["https://api.dezrez.com/api/simplepropertyrole/search?APIKEY=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguZGV6cmV6LmNvbS9BcGlLZXlJc3N1ZXIiLCJhdWQiOiJodHRwczovL2FwaS5kZXpyZXouY29tL3NpbXBsZXdlYmdhdGV3YXkiLCJuYmYiOjE1Mjc4NDA4ODcsImV4cCI6NDY4MzUxNDQ4NywiSXNzdWVkVG9Hcm91cElkIjoiNjMzMTIzMyIsIkFnZW5jeUlkIjoiMzg4Iiwic2NvcGUiOlsiaW1wZXJzb25hdGVfd2ViX3VzZXIiLCJwcm9wZXJ0eV9iYXNpY19yZWFkIiwibGVhZF9zZW5kZXIiXX0.UJ2MNlksjyXVpB7qsRkPtC5u2JxOPAWeko0FKHVq6sE"]
    
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Origin": "https://www.locationestateagency.co.uk",
        "Referer": "https://www.locationestateagency.co.uk/",
        "Rezi-Api-Version": "1.0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
    }
    
    def start_requests(self):
        start_urls = [
            {
                "payload": [
                    '{"MinimumPrice":"0","MaximumPrice":"9999999","PageNumber":1,"PageSize":12,"RoleTypes":["Letting"],"MarketingFlags":["ApprovedForMarketingWebsite"],"PropertyTypes":["Flat","GroundFloorFlat"],"IncludeStc":true}',
                ],
                "property_type": "apartment"
            },
	        {
                "payload": [
                    '{"MinimumPrice":"0","MaximumPrice":"9999999","PageNumber":1,"PageSize":12,"RoleTypes":["Letting"],"MarketingFlags":["ApprovedForMarketingWebsite"],"PropertyTypes":["SemiDetachedHouse","SemiDetachedBungalow","SemiDetachedVilla"],"IncludeStc":true}',
                    '{"MinimumPrice":"0","MaximumPrice":"9999999","PageNumber":1,"PageSize":12,"RoleTypes":["Letting"],"MarketingFlags":["ApprovedForMarketingWebsite"],"PropertyTypes":["TerracedHouse","EndTerraceHouse","SemiDetachedHouse","DetachedHouse"],"IncludeStc":true}',
                    '{"MinimumPrice":"0","MaximumPrice":"9999999","PageNumber":1,"PageSize":12,"RoleTypes":["Letting"],"MarketingFlags":["ApprovedForMarketingWebsite"],"PropertyTypes":["DetachedHouse","DetachedBungalow","DetachedVilla"],"IncludeStc":true}',
                    '{"MinimumPrice":"0","MaximumPrice":"9999999","PageNumber":1,"PageSize":12,"RoleTypes":["Letting"],"MarketingFlags":["ApprovedForMarketingWebsite"],"PropertyTypes":["TerracedHouse","TerracedBungalow","EndTerraceHouse"],"IncludeStc":true}',
                    '{"MinimumPrice":"0","MaximumPrice":"9999999","PageNumber":1,"PageSize":12,"RoleTypes":["Letting"],"MarketingFlags":["ApprovedForMarketingWebsite"],"PropertyTypes":["TerracedBungalow","SemiDetachedBungalow","DetachedBungalow"],"IncludeStc":true}',
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('payload'):
                yield Request(
                    url=self.post_urls[0],
                    callback=self.parse,
                    method="POST",
                    body=item,
                    headers=self.headers,
                    meta={'property_type': url.get('property_type'), "payload": item}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        
        data = json.loads(response.body)
        for item in data["Collection"]:
            external_id = item["RoleId"]
            follow_url = f"https://www.locationestateagency.co.uk/property-details/?pid={external_id}&branch=mansfield"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "item":item})
            seen = True
        if page ==2 or seen:
            payload = response.meta.get('payload').replace(f'PageNumber":{page-1}', f'"PageNumber":{page}').replace('"RoleTypes":["Letting"]', '"RoleTypes":""')
            print(payload)
            yield Request(
                    url=self.post_urls[0],
                    callback=self.parse,
                    method="POST",
                    body=payload,
                    headers=self.headers,
                    meta={'property_type': response.meta.get('property_type'), "payload": payload}
                )
            
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta.get('item')

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source",self.external_source)

        external_id = item['RoleId']
        item_loader.add_value("external_id", str(external_id))

        address_street = item['Address']['Street']
        address_city = item['Address']['Town']
        address_zipcode = item['Address']['Postcode']

        address = address_street + " " + address_city + " " + address_zipcode
        item_loader.add_value("title", address)
        item_loader.add_value("address", address)
        item_loader.add_value("city", address_city)
        item_loader.add_value("zipcode", address_zipcode)

        rent = item['Price']['PriceValue']
        if rent:
            rent = str(rent).split(".")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        room_count = item['RoomCountsDescription']['Bedrooms']
        item_loader.add_value("room_count", room_count)
        bathroom_count = item['RoomCountsDescription']['Bathrooms']
        item_loader.add_value("bathroom_count", bathroom_count)

        desc = item['SummaryTextDescription']
        item_loader.add_value("description", desc)            

        for image in item['Images']:
            item_loader.add_value("images", image["Url"])

        parking = item['AmenityDescription']['ParkingSpaces']
        garage = item['AmenityDescription']['Garages']
        if parking > 0:
            item_loader.add_value("parking", True)
        elif garage > 0:
            item_loader.add_value("parking", True)
        
        features = item['Descriptions']
        for i in features:
            try:                
                deposit = str(i['Features'])
                if "Bond" in deposit:
                    deposit = deposit.split("Bond")[1].split("'")[0].replace("£","").strip()
                    item_loader.add_value("deposit", deposit)
            except :
                pass
        if item['Address']['Location']:
            latitude = item['Address']['Location']['Latitude']
            longitude = item['Address']['Location']['Longitude']

            item_loader.add_value("latitude", str(latitude))
            item_loader.add_value("longitude", str(longitude))

        landlord_name = item['OwningTeam']['Name']
        landlord_email = item['OwningTeam']['Email']
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", "01623 654555")
    
        status = item['RoleStatus']['DisplayName']
        if "to let" in status.lower():
            yield item_loader.load_item()