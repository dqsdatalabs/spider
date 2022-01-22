# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy
import re

class MySpider(Spider):
    name = 'creativeproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'authority': 'creativeproperty.com.au',
        'accept': 'application/json, text/plain, */*',
        'referer': 'https://creativeproperty.com.au/?/rent/residential',
        'accept-language': 'tr,en;q=0.9',
        'Cookie': '__cfduid=d5739e28619fddb71959ad5016a7006171615442783; CFID=58941152; CFTOKEN=ed10837381611ee2-3EC73CCD-C19B-77AF-D57506579472F809'
    }

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://creativeproperty.com.au/?json/listing/page/1/perPage/3/orderby/new-old/filtertype/residentialRental/solddays/90/leaseddays/0/restype/5,6/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://creativeproperty.com.au/?json/listing/page/1/perPage/3/orderby/new-old/filtertype/residentialRental/solddays/90/leaseddays/0/restype/7,9,39,40,11,15/",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://creativeproperty.com.au/?json/listing/page/1/perPage/3/orderby/new-old/filtertype/residentialRental/solddays/90/leaseddays/0/restype/1/",
                ],
                "property_type" : "studio",
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
        selector = scrapy.Selector(text=data["BODY"], type="html")
        for item in selector.xpath("//div[contains(@id,'listing-')]/a/@href").getall():
            seen = True
            follow_url = "https://creativeproperty.com.au/" + item.replace("\\", "").replace('"', "")
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace("page/" + str(page - 1), "page/" + str(page))
            yield Request(follow_url, headers=self.headers, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Creativeproperty_Com_PySpider_australia")
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//title/text()").get()
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
        
        item_loader.add_xpath("city", "//h1/strong/text()")

        zipcode = response.xpath("//meta[@name='Description']/@content").get()
        if zipcode: item_loader.add_value("zipcode", " ".join(zipcode.strip().split(" ")[-2:]).strip())
        
        rent = response.xpath("//h4/text()").get()
        if rent:
            price = rent.split(" ")[0].replace("$","").split(".")[0]
            item_loader.add_value("rent", int(float(price))*4)
        item_loader.add_value("currency", "AUD")
        
        room_count = response.xpath("//i[contains(@class,'bed')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        room_count = response.xpath("//i[contains(@class,'bath')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("bathroom_count", room_count.strip())
        
        parking = response.xpath("//i[contains(@class,'-car')]/following-sibling::text()").get()
        if parking and parking.strip() !="0":
            item_loader.add_value("parking", True)
        
        import dateparser
        available_date = response.xpath("//i[contains(@class,'calendar')]/following-sibling::span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        external_id = response.xpath("substring-after(//span[contains(.,'ID')]/text(),':')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        deposit = response.xpath("//span[contains(.,'Bond')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("$",""))
        
        description = " ".join(response.xpath("//div[contains(@class,'border-bot')]//p//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip().replace("*","").replace("-",""))
            item_loader.add_value("description", description.strip())
        
        if "sqm" in description:
            item_loader.add_value("square_meters", description.split("sqm")[0].strip().split(" ")[-1])
        
        images = [x for x in response.xpath("//div[contains(@class,'photoswipe-thumbnail-image')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//iframe/@src[contains(.,'map')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('&q=')[1].split(',')[0]
            longitude = latitude_longitude.split('&q=')[1].split(',')[1]      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        floor_plan_images = response.xpath("//img[contains(@alt,'Floorplan')]/@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        item_loader.add_value("landlord_name", "Creative Property Co.")
        item_loader.add_value("landlord_phone", "02 4955 6900")
        item_loader.add_value("landlord_email", "admin@creative.property")

        if not item_loader.get_collected_values("zipcode"):
            zipcode = response.xpath("//meta[@name='Description']/@content").get()
            if zipcode: item_loader.add_value("zipcode", " ".join(zipcode.strip().split(" ")[-2:]).strip())
        
        yield item_loader.load_item()