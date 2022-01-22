# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'raywhitesurryhills_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Raywhitesurryhills_Com_PySpider_australia'
    custom_settings = {
        "PROXY_ON":"True"
    }

    url = "https://raywhiteapi.ep.dynamics.net/v1/listings?apiKey=FB889BB8-4AC9-40C2-829A-DD42D51626DE"
    headers = {
        'Connection': 'keep-alive',
        'Accept': 'application/json',
        'X-ApiKey': 'FB889BB8-4AC9-40C2-829A-DD42D51626DE',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.41 YaBrowser/21.2.0.1097 Yowser/2.5 Safari/537.36',
        'Content-Type': 'application/json',
        'Origin': 'https://raywhitesurryhills.com.au',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://raywhitesurryhills.com.au/properties/residential-for-rent?category=APT%7CSTD%7CHSE%7CTCE%7CUNT&keywords=&minBaths=0&minBeds=0&minCars=0&rentPrice=&sort=updatedAt+desc&suburbPostCode=',
        'Accept-Language': 'tr,en;q=0.9'
    }
    payload = "{\"size\":50,\"statusCode\":\"CUR\",\"typeCode\":\"REN\",\"categoryCode\":[\"APT\",\"STD\",\"HSE\",\"TCE\",\"UNT\"],\"sort\":[\"updatedAt desc\",\"id desc\",\"_score desc\"],\"organisationId\":[1935,1808,3224],\"from\":0}"

    def start_requests(self):
        yield Request(self.url, method="POST", headers=self.headers, body=self.payload, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 50)
        seen = False

        data = json.loads(response.body)
        for item in data["data"]:
            seen = True
            property_type = item["value"]["categories"][0]["code"]
            if property_type:
                if property_type.strip() in ["APT", "UNT"]: property_type = "apartment"
                elif property_type.strip() in ["HSE", "TCE"]: property_type = "house"
                elif property_type.strip() in ["STD"]: property_type = "studio"
                else: continue
            landlord_name = item["value"]["agents"][0]["fullName"]
            landlord_phone = item["value"]["agents"][0]["mobilePhone"]
            landlord_email = item["value"]["agents"][0]["email"]
            external_id = item["value"]["id"]
            # latitude = item["value"]["address"]["location"]["lat"]
            # longitude = item["value"]["address"]["location"]["lon"]
            follow_url = "https://raywhitesurryhills.com.au/properties/residential-for-rent/" + str(external_id)
            if property_type: 
                yield Request(follow_url, callback=self.populate_item, meta={
                    "property_type":property_type,
                    "landlord_name":landlord_name,
                    "landlord_phone":landlord_phone,
                    "landlord_email":landlord_email,
                    "external_id":str(external_id),
                    # "latitude":str(latitude),
                    # "longitude":str(longitude),
                    })
        
        if page == 50 or seen:
            self.payload = self.payload.replace('from":' + str(page - 50), 'from":' + str(page))
            yield Request(self.url, method="POST", headers=self.headers, body=self.payload, callback=self.parse, meta={"page": page + 50})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("landlord_name", response.meta["landlord_name"])
        item_loader.add_value("landlord_phone", response.meta["landlord_phone"])
        item_loader.add_value("landlord_email", response.meta["landlord_email"])
        item_loader.add_value("external_id", response.meta["external_id"])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.external_source, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1[contains(@class,'pdp_address')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1[contains(@class,'pdp_address')]/span/text()", input_type="F_XPATH", split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1[contains(@class,'pdp_address')]/span/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'pdp_price')]/text()[not(contains(.,'Price'))]", input_type="F_XPATH", get_num=True, per_week=True,split_list={" ":0}, replace_list={"$":"", ",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(@class,'pdp_price')]/text()[contains(.,'/')]", input_type="F_XPATH", get_num=True,split_list={"/":1, " ":0}, replace_list={"$":"", ",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="USD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Bedroom')]/parent::div/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(.,'Building')]/parent::div/following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={"m²":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'Bathroom')]/parent::div/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        
        desc = " ".join(response.xpath("//div[@class='pdp_description_content']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        latitude_longitude = response.xpath("//script[contains(.,'longitude')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"latitude":')[1].split(",")[0]
            longitude = latitude_longitude.split('"longitude":')[1].split("}")[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        import dateparser
        month = "".join(response.xpath("//div[contains(@class,'available')]//span[@class='event_month']//text()").getall())
        day = response.xpath("//div[contains(@class,'available')]//span[@class='event_date']//text()").get()
        if day or month:
            available_date = day + " " + month
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1].replace("th","").replace("rd","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
                
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//noscript//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[contains(.,'Parking')]/parent::div/following-sibling::div//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrace')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Lift')]/text()", input_type="F_XPATH", tf_item=True)
        # ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value=response.meta["latitude"], input_type="VALUE")
        # ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value=response.meta["longitude"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[contains(@class,'available')]//div[@class='event_date_wrap']//text()", input_type="M_XPATH")

        yield item_loader.load_item()