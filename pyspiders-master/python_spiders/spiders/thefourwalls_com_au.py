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
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'thefourwalls_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://thefourwalls.com.au/?json/listing/restype/5,40/orderby/new-old/page/{}/filterType/residentialRental/solddays/120/leaseddays/120",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://thefourwalls.com.au/?json/listing/restype/6,7,9,39,11,15/orderby/new-old/page/{}/filterType/residentialRental/solddays/120/leaseddays/120",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data = json.loads(response.body)    
        body_html = Selector(text=data["BODY"], type="html")
        for item in body_html.xpath("//a[@class='listing-item']"):
            status = item.xpath(".//div[@class='listing-banner']/text()").get()
            if status and "leased" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base = response.meta["base"]
            p_url = base.format(page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "base":base, "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Thefourwalls_Com_PySpider_australia") 

        external_id = response.xpath("//span[contains(.,'ID')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)
                
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = " ".join(response.xpath("//h1//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)

        city = response.xpath("//h1//strong//text()").get()
        if city:
            item_loader.add_value("city",city)
        
        zipcode = response.xpath("//meta[contains(@name,'Description')]//@content").get()
        if zipcode:
            zipcode_ = zipcode.strip().split(" ")[-2] + " " + zipcode.strip().split(" ")[-1]
            item_loader.add_value("zipcode", zipcode_)

        desc = " ".join(response.xpath("//div[contains(@class,'description')]//div[contains(@class,'inner')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = "".join(response.xpath("//div[contains(@class,'bed')]/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count = "".join(response.xpath("//div[contains(@class,'bath')]/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        parking = "".join(response.xpath("//div[contains(@class,'car')]/text()").getall())
        if parking:
            item_loader.add_value("parking", True)
        
        rent = response.xpath("//h4//text()").get()
        if rent:
            rent = rent.split("$")[1].strip().split(" ")[0].split(".")[0]
            item_loader.add_value("rent", int(float(rent))*4)
        deposit = response.xpath("//span[contains(.,'Bond')]//span//text()").get()
        if deposit:
            deposit = deposit.split("$")[1].strip()
            item_loader.add_value("deposit", deposit)
        item_loader.add_value("currency", "AUD")
                
        images = [x for x in response.xpath("//div[contains(@class,'gallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//h5[contains(.,'Available')]//following-sibling::div//text()").getall())
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                available_date = response.xpath("//i[contains(@class,'calendar')]//following-sibling::span//text()").get()
                if available_date:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
        
        swimming_pool = response.xpath("//span[contains(.,'Pool')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        furnished = response.xpath("//span[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        dishwasher = response.xpath("//span[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        balcony = response.xpath("//span[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        latitude_longitude = response.xpath("//iframe[contains(@class,'gmapIFrame')]//@src").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('=')[-1].split(',')[0]
            longitude = latitude_longitude.split('=')[-1].split(',')[1].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "FOURWALLS REALTY")
        item_loader.add_value("landlord_phone", "07 4153 6444")
        item_loader.add_value("landlord_email", "rentals@thefourwalls.com.au ")
        
        yield item_loader.load_item()
