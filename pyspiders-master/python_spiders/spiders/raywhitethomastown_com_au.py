# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector 
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re 
import json

class MySpider(Spider):
    name = 'raywhitethomastown_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source="Raywhitethomastown_Com_PySpider_australia"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://raywhitethomastown.com.au/properties/residential-for-rent?category=APT%7CUNT&keywords=&minBaths=0&minBeds=0&minCars=0&rentPrice=&sort=updatedAt+desc&suburbPostCode=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://raywhitethomastown.com.au/properties/residential-for-rent?category=HSE%7CTHS&keywords=&minBaths=0&minBeds=0&minCars=0&rentPrice=&sort=updatedAt+desc&suburbPostCode=",
                ],
                "property_type" : "house"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, 
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='proplist_item']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        external_id = response.url.split("/")[-1]
        item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address.strip())

        city_zipcode = response.xpath("//h1//span//text()").get()
        if city_zipcode:
            city = city_zipcode.split(",")[0]
            zipcode = city_zipcode.split(",")[-1]
            item_loader.add_value("city", city.strip()) 
            item_loader.add_value("zipcode", zipcode.strip())

        rent_deposit = response.xpath("//div[contains(@class,'pdp_header')]//span[contains(@class,'price')]//text()").get()
        if rent_deposit and not "per week" in rent_deposit:
            rent = rent_deposit.split("w")[-1].split(".")[0].split("pcm")[0].replace("$","").strip().replace(".","").replace(",","")
            deposit = rent_deposit.split("pcm")[-1].split("/")[1].replace("$","").strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
            item_loader.add_value("deposit", deposit)
        else:
            rent1=response.xpath("//div[contains(@class,'pdp_header')]//span[contains(@class,'price')]//text()").get()
            deposit=rent1.split("pcm")[-1].split("/")[1].replace("$","").strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent",int(rent1.split("/")[0].split("$")[1].split(" ")[0])*4)
            item_loader.add_value("deposit",deposit)
        

        item_loader.add_value("currency", "AUD")

        desc = " ".join(response.xpath("//div[contains(@class,'pdp_description_content')]/p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'pdp_features_table')]//div[contains(@class,'tbc top')][contains(.,'Bedroom')]//following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[contains(@class,'pdp_features_table')]//div[contains(@class,'tbc top')][contains(.,'Bathroom')]//following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'pdp_image')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//span[contains(@class,'event_date')]//text()").get()
        available_month = "".join(response.xpath("//span[contains(@class,'event_month')]//text()").getall())
        if available_date and available_month:
            available = available_date+" "+available_month
            if not "now" in available.lower():
                date_parsed = dateparser.parse(available, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//div[contains(@class,'pdp_features_table')]//div[contains(@class,'tbc top')][contains(.,'Parking')]//following-sibling::div//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        latitude_longitude = response.xpath("//script[contains(.,'GeoCoordinates')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude":')[1].split(',')[0].strip()  
            longitude = latitude_longitude.split('longitude":')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        latitudecheck=item_loader.get_output_value("latitude")
        if not latitudecheck:
            latitude=response.xpath("//script[contains(.,'latitude')]/text()").get()
            if latitude:
                latitude=latitude.split("latitude")[-1].split(",")[0].split(":")[-1]
                item_loader.add_value("latitude", latitude)
            longitude=response.xpath("//script[contains(.,'latitude')]/text()").get()
            if longitude:
                longitude=longitude.split("longitude")[-1].split(",")[0].split(":")[-1]
                item_loader.add_value("longitude", longitude)


        item_loader.add_value("landlord_name", "Ray White Thomastown")
        item_loader.add_value("landlord_phone", "+61 (3) 9465 2344")
        item_loader.add_value("landlord_email", "thomastown.vic@raywhite.com")

        yield item_loader.load_item()