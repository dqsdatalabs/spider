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
import re

class MySpider(Spider):
    name = 'sydneylinks_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source = "Sydneylinks_Com_PySpider_australia"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.sydneylinks.com.au/rent/properties-for-lease/page/{}/?property_type%5B0%5D=Apartment&property_type%5B1%5D=Unit&min_price&max_price&bedrooms&bathrooms&carspaces",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.sydneylinks.com.au/rent/properties-for-lease/page/{}/?property_type%5B%5D=House&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.sydneylinks.com.au/rent/properties-for-lease/page/{}/?property_type%5B0%5D=Studio&min_price&max_price&bedrooms&bathrooms&carspaces",
                ],
                "property_type" : "studio"
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'container')]/a"):
            status = item.xpath(".//div[@class='sticker']/text()").get()
            if status and "leased" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                    "page":page+1,
                    "base_url":base_url,
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//li[contains(.,'ID')]//div//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'address')]//text()").get()
        if address:
            city = address.split(",")[-1].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)  

        rent = response.xpath("//div[contains(@class,'price')]//text()").get()
        if rent:
            if "deposit" in rent.lower():
                return
            price= rent.replace("pw","").split("- ")[0].split("$")[-1].strip().split(" ")[0]
            item_loader.add_value("rent", int(price)*4)
        item_loader.add_value("currency", "AUD")

        room_count = response.xpath("//li[contains(.,'Bed')]//div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[contains(.,'Bath')]//div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//li[contains(.,'Garage')]//div//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]//div//text()").get()
        if available_date:
            if "now" not in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        desc = " ".join(response.xpath("//div[contains(@class,'detail-description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        elevator = response.xpath("//li[contains(.,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        furnished = response.xpath("//li[contains(.,'Furnished')]//text() | //h5[@class='sub-title']/text()[contains(.,'FURNISHED')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        
        

        images = [x for x in response.xpath("//div[contains(@class,'slide')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        if "floor " in desc:
                floor = desc.split("floor ")[0].strip().split(" ")[-1]
                if "heated" in floor.lower():
                    floor = desc.split("floor ")[1].strip().split(" ")[-1]
                not_list = ["new","the","whole"]
                status = True
                for i in not_list:
                    if i in floor.lower():
                        status = False
                if status:
                    item_loader.add_value("floor", floor)

        landlord_name = response.xpath("//div[contains(@class,'agent-detail')]//strong//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//div[contains(@class,'agent-detail')]//p[contains(@class,'email')]/a/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.strip())
        
        landlord_phone = response.xpath("//div[contains(@class,'agent-detail')]//p[contains(@class,'phone')]/a/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())

        yield item_loader.load_item()