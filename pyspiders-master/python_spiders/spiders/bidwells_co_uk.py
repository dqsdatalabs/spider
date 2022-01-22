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
    name = 'bidwells_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="Bidwells_Co_PySpider_united_kingdom"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://homes.bidwells.co.uk/residential-property-search/?LatLng=56.1840400786807%2C-2.15100688066327&Bounds=48.648047985031866%2C-11.33010369078724%2C63.72003217232951%2C7.028089929460691&MapView=False&s=True&SearchType=1&LocationOrProperty=Location&Location=UK&PropertyName=&OrderBy=4&SearchType=1&SearchRadius=&PropertyType=Apartment%2FFlat&MinPrice=&MaxPrice=&MinRentalPrice=&MaxRentalPrice=&MinBeds=1&Tenure=Any",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://homes.bidwells.co.uk/residential-property-search/?LatLng=56.1840400786807%2C-2.15100688066327&Bounds=48.648047985031866%2C-11.33010369078724%2C63.72003217232951%2C7.028089929460691&MapView=False&s=True&SearchType=1&LocationOrProperty=Location&Location=UK&PropertyName=&OrderBy=4&SearchType=1&SearchRadius=&PropertyType=House&PropertyType=Bungalow&MinPrice=&MaxPrice=&MinRentalPrice=&MaxRentalPrice=&MinBeds=1&Tenure=Any",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//li/div//a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[.='Next']/@href").get()
        if next_page:
            yield Request( 
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})    
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//h2[@class='h3']/text()").get()
        if 'pw' not in status.lower() or 'pcm' not in status.lower():
            return
        dontsold=response.xpath("//div[@class='absolute top-5 left-5 flex flex-wrap']/div/text()").get()
        if dontsold and "sold" in dontsold.lower():
            return
        status=response.xpath("//div[@class='bg-zodiacblue text-white eyebrow px-3 pt-4 pb-3 mr-3 flex items-center']/text()").get()
        if status and "Let agreed"==status:
            return 
 
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        item_loader.add_value("external_source","Bidwells_Co_PySpider_united_kingdom")
        # ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Bidwells_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='col-start-3 lg:col-start-3 col-span-20 lg:col-span-15']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(text(),'sq ft')]/text()", input_type="F_XPATH", get_num=True, split_list={"sq ft":0, " ":-1}, sq_ft=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Bedroom')]/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'Bathroom')]/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div/h2/text()[contains(.,'£')]", input_type="F_XPATH", get_num=True, split_list={"pcm":0}, lower_or_upper=0)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//text()[contains(.,'Available immediately')]", input_type="F_XPATH", replace_list={"Available immediately":"now"})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//text()[contains(.,'Holding Deposit')]", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//text()[contains(.,'Rent:') and contains(.,'Unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//text()[contains(.,'Rent:') and contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[1]/div[span[.='Property Advisor']]/../following-sibling::ul[1]/li/a[contains(@href,'tel')]/text()", input_type="F_XPATH", split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[1]/div[span[.='Property Advisor']]/../following-sibling::ul[1]/li/a[contains(@href,'mail')]/text()", input_type="F_XPATH", split_list={":":-1})
        landlord_name = response.xpath("//div[1]/div[span[.='Property Advisor']]/h5/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        else:
            item_loader.add_value("landlord_name", "Bidwells")
        phonecheck=item_loader.get_output_value("landlord_phone")
        if not phonecheck: 
            if "Ellie Lucas" in landlord_name:
                item_loader.add_value("landlord_phone","0333 363 42 71")

        address = response.xpath("//h1/text()").get() 
        if address:
            if "Cambridge" in address:
                item_loader.add_value("city", "Cambridge")
            else:
                city = address.split(",")[1]
                if "Road" not in city and " Way" not in city:
                    item_loader.add_value("city", city.strip())
                else:
                    item_loader.add_value("city", address.split(",")[2].strip())

        # data = json.loads(response.xpath("//script[contains(.,'mapData')]/text()").get().split('mapData =')[-1].strip().strip(';').strip())
        # latitude = None
        # longitude = None
        
        # for item in data["features"]: 
        #     if item["properties"]["items"][0]["current"] == True:
        #         latitude = item["properties"]["items"][0]["Lat"]
        #         longitude = item["properties"]["items"][0]["Lon"]
        # if not latitude:
        #     for item in data["features"]: 
        #         longitude = item["geometry"]["coordinates"][0]
        #         latitude = item["geometry"]["coordinates"][1]
        # item_loader.add_value("latitude", str(latitude))
        # item_loader.add_value("longitude", str(longitude))
     
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'relative h-full overflow-hidden')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        yield item_loader.load_item()