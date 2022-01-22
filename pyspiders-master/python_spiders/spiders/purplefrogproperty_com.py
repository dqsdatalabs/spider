# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'purplefrogproperty_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    custom_settings = {
        "CONCURRENT_REQUESTS" : 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
        "PROXY_ON" : True
    }
    headers = {
        'accept': ' application/json, text/javascript, */*; q=0.01',
        'accept-encoding': ' gzip, deflate, br',
        'accept-language': ' tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
        'sec-fetch-dest': ' empty',
        'sec-fetch-mode': ' cors',
        'sec-fetch-site': ' same-origin',
        'x-requested-with': ' XMLHttpRequest',
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.purplefrogproperty.com/student-accommodation/",
                ],
                "property_type" : "student_apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            headers=self.headers,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 12)
        seen = False
        try:
            data = json.loads(response.body)
        
            for item in data["properties"]:
                follow_url = f"https://www.purplefrogproperty.com/student-accommodation/property-{item['id']}"
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"],"item":item})
                seen = True
                
        except:
            for item in response.xpath("//div[@class='location']/a/@href").getall():
                yield Request(response.urljoin(item), callback=self.populate_item2, meta={"property_type":response.meta.get('property_type')})
                seen = True
        
        if page==12 or seen:
            url = f"https://www.purplefrogproperty.com/student-accommodation/?&page={page}&"
            yield Request(url, callback=self.parse, headers=self.headers, meta={"page":page+12, "property_type":response.meta.get("property_type")})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta.get('item')

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Purplefrogproperty_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_id", item["id"])
        rent = "".join(response.xpath("//div/span[@class='price']/text()").extract())
        if rent:
            price = rent.split("£")[1].strip()
            item_loader.add_value("rent",int(float(price)*4))

        item_loader.add_value("currency", "GBP")
        item_loader.add_value("address", "{} {} {}".format(item["address_1"],item["address_2"],item["address_3"]))
        item_loader.add_value("zipcode", item["post_code"])
        item_loader.add_value("city", item["address_3"])
        item_loader.add_value("description", item["description"])
        item_loader.add_value("bathroom_count", item["showers"])
        item_loader.add_value("deposit", item["required_deposit"])
        item_loader.add_value("latitude", str(item["location"]["lat"]))
        item_loader.add_value("longitude", str(item["location"]["lon"]))
        item_loader.add_value("energy_label", item["epc_rating"])

        room_count = item["bedrooms"]
        if int(room_count) > 1 and item_loader.get_collected_values("rent")[0]<650:
            item_loader.add_value("property_type", "room")
            item_loader.add_value("room_count", "1")
        elif room_count == 1:
            item_loader.add_value("property_type", "room")
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("room_count", room_count)
        
        images = [x for x in response.xpath("//section[@id='property-images']/div/div//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = item["furnished"]
        if furnished:
            item_loader.add_value("furnished",True if furnished !="0" else False)

        features = item["features"]
        if features:
            item_loader.add_value("washing_machine",True if "washingmachine" in features else False)
            item_loader.add_value("dishwasher",True if "dishwasher" in features else False)

        images = [x for x in item["images"]]
        if images:
            item_loader.add_value("images", images)

        available_date=item["available_from"]
        if available_date:
            date2 = available_date.split(" ")[0].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%d-%m-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        item_loader.add_value("landlord_phone", "0121 270 2722")
        item_loader.add_value("landlord_name", "Purple Frog")
        item_loader.add_value("landlord_email", "birmingham@purplefrogproperty.com")

        yield item_loader.load_item()

    def populate_item2(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Purplefrogproperty_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("title", address)
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2])
            item_loader.add_value("zipcode", address.split(",")[-1])
        
        external_id = response.xpath("//div[contains(@class,'ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='price']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li/span[contains(.,'Available')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li/span[contains(.,'Deposit')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[@class='beds']//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[@class='showers']//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='description']//p//text()", input_type="M_XPATH")
                
        energy_label = response.xpath("//h5[contains(.,'Energy')]/text()").get()
        if energy_label:
            energy_label = energy_label.split("-")[1].strip()
            item_loader.add_value("energy_label", energy_label)
            
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div/@data-lat", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div/@data-lng", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='features']/i[contains(.,'Furnished') or contains(.,' furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//div[@class='features']/i[contains(.,'Washing') or contains(.,'Washer')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//div[@class='features']/i[contains(.,'Dishwasher')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Purple Frog", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0121 270 2722", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="birmingham@purplefrogproperty.com", input_type="VALUE")
        
        yield item_loader.load_item()