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
    name = 'metrorentals_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'   
    custom_settings = {"HTTPCACHE_ENABLED": False}
    headers = {
        'authority': 'www.domain.com.au',
        'cache-control': 'max-age=0',
        'accept': 'application/json',
        'accept-language': 'tr,en;q=0.9',
    }

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.domain.com.au/rent/?ptype=apartment&excludedeposittaken=1&page=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.domain.com.au/rent/?ptype=duplex,free-standing,new-home-designs,new-house-land,semi-detached,terrace,town-house,villa&excludedeposittaken=1&page=1",
                ],
                "property_type" : "house"
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

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        for k, v in data["props"]["listingsMap"].items():
            seen = True
            yield Request(response.urljoin(v["listingModel"]["url"]), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace("&page=" + str(page - 1), "&page=" + str(page))
            yield Request(follow_url, headers=self.headers, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        prop_type = response.xpath("//span[contains(@class,'css-in3yi3')][contains(.,'Studio')]//text()").get()
        if prop_type:
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta["property_type"])

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Metrorentals_Com_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("-")[-1])

        title = response.xpath("//title//text()").get()
        item_loader.add_value("title", title)
        rent = response.xpath("//div[contains(@data-testid,'title')]//text()[contains(.,'$')]").get()
        if rent:
            if "per week" in rent.lower() or "p/w" in rent.lower() or "pw" in rent.lower() or "wk" in rent.lower() or "weekly" in rent.lower() or "p.w." in rent.lower():
                price = rent.lower().replace("per"," ").replace("pw"," ").replace("-"," ").replace("p/"," ").replace("/"," ").split(".")[0].replace(",","").split("$")[1].strip().split(" ")[0]
                try:
                    rent = int(price)*4
                except:
                    price = rent.split("$")[0].strip()
                    rent = int(price)*4
            else:
                rent = rent.lower().split(".")[0].replace(",","").split("$")[1].strip().split(" ")[0]
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "USD")

        address = response.xpath("//h1//text()").get()
        if address:
            zipcode = address.strip().split(" ")[-1]
            item_loader.add_value("zipcode", zipcode)
            if "St " in address:
                city = address.split("St ")[1].split("QLD")[0]
                item_loader.add_value("city", city)
            else:
                city = address.split("QLD")[0].strip().split(" ")[-1]
                item_loader.add_value("city", city)
            item_loader.add_value("address", address)
        
        room_count = response.xpath("//span/span[contains(.,'Bed')]/parent::span[@class='css-1rzse3v']/text()").get()
        if room_count:
            if room_count.strip() != "0":
                item_loader.add_value("room_count", room_count)
            elif response.xpath("//span[contains(@class,'css-in3yi3')][contains(.,'Studio')]//text()").get():
                item_loader.add_value("room_count", "1")
                

        bathroom_count = response.xpath("//div[contains(@data-testid,'listing-details__summary-left-column')]//span[contains(@class,'css-1rzse3v')][contains(.,'Bath')]/text()").get()
        if bathroom_count and bathroom_count.isdigit():
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = " ".join(response.xpath("//span[contains(@class,'css-1rzse3v')][contains(.,'Park')]//text()").getall())
        if parking:
            parking = re.sub('\s{2,}', ' ', parking.strip())
            parking2 = parking.split("Parking")[0].strip().split(" ")[0]
            if "−" in parking2:
                parking2 = parking.split("Parking")[1].strip().split(" ")[0]
                if parking !='0':
                    item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", True)

        import dateparser
        available_date = "".join(response.xpath("//li[contains(.,'Available')]//strong//text()[not(contains(.,'Now')) and not(contains(.,'now'))]").getall())
        if available_date:
            if "now" not in available_date.lower():
                available_date = available_date.split(",")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//li[contains(.,'Bond')]//strong//text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("$")[1].strip())
        
        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        furnished = response.xpath("//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        swimming_pool = response.xpath("//li[contains(.,'Swimming Pool')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        floor = response.xpath("//li[contains(.,' floor')]//text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("floor")[0].strip())
        
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//p//text()").getall())        
        desc2 = response.xpath("//script[contains(.,'description\":[\"')]/text()").get()
        if desc or desc2:
            description = ""
            if desc2:
                description += desc2.split('description":["')[1].split('"]')[0]
            if desc:
                description += re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", description)
        
        images = response.xpath("//script[contains(.,'images')]/text()").get()
        if images:
            image_size = images.split('gallery":{"slides"')[1].split('header":{"')[0].strip().split('{"url":"')
            for i in range(1,len(image_size)):
                item_loader.add_value("images", images.split('{"url":"')[i].split('"')[0])

        latitude_longitude = response.xpath("//script[contains(.,'latitude')]/text()").get()  
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude":')[1].split(',')[0]
            longitude = latitude_longitude.split('longitude":')[1].split(',')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "DOMAİN GROUP")
        item_loader.add_value("landlord_phone", "1300 799 109")
        item_loader.add_value("landlord_email", "support@domain.com.au")

        yield item_loader.load_item()