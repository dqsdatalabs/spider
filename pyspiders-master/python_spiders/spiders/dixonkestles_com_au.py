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
    name = 'dixonkestles_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    custom_settings = {
        "PROXY_ON" : "True"
    }

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.dixonkestles.com.au/?json/listing/orderby/new-old/restype/5/page/1/filterType/residentialRental/leased/false/solddays/365/leaseddays/365",
                    
                ],
                "property_type" : "apartment"
            },
            {
                "url": [
                    "https://www.dixonkestles.com.au/?json/listing/orderby/new-old/restype/9/page/1/filterType/residentialRental/leased/false/solddays/365/leaseddays/365",
                ],
                "property_type" : "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(url=item,
                                callback=self.parse,
                                meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False

        data = json.loads(response.body)
        data = data["ITEMSHTML"]
        for item in data:
            sel = Selector(text=item, type='html')
            follow_url = sel.xpath("//a[@class='listing-item']/@href").get()
            yield Request(response.urljoin(follow_url), callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})
            seen=True
        
        if page ==2 or seen:        
            f_url = response.url.replace(f"page/{page-1}", f"page/{page}")
            yield Request(f_url, callback=self.parse, meta={"page": page+1, 'property_type':response.meta.get('property_type')})

#     # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Dixonkestles_PySpider_australia")

        external_id = response.xpath("//span[contains(.,'Property ID')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = "".join(response.xpath("//h1//span//text() | //h1//strong//text()").getall())
        if title:
            item_loader.add_value("title", title)
        

        address = "".join(response.xpath("//h1//span//text() | //h1//strong//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//h1//strong//text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        zipcode=response.url
        if zipcode:
            item_loader.add_value("zipcode","".join(zipcode.split("-")[-2:]).upper())

        square_meters = response.xpath("//span[contains(.,'Land Size')]//span/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0].split(".")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//h4//text()").get()
        if rent:
            rent = rent.strip().replace("$","").split(" ")[0]
            rent = int(float(rent))*4
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")

        deposit = response.xpath("//span[contains(.,'Bond')]//span//text()").get()
        if deposit:
            deposit = deposit.strip().replace("$","")
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'property-description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = "".join(response.xpath("//div[contains(@class,'bed')]//text()").getall())
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = "".join(response.xpath("//div[contains(@class,'bath')]//text()").getall())
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//meta[@property='og:image']//@content").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//h5[contains(.,'Available Date')]//following-sibling::div//span//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = "".join(response.xpath("//div[contains(@class,'car')]//text()").getall())
        if parking:
            item_loader.add_value("parking", True)

        latitude_longitude = response.xpath("//div[contains(@class,'gmapDiv')]//@src").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('&q=')[1].split(',')[0]
            longitude = latitude_longitude.split('&q=')[1].split(',')[1].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)        

        landlord_name = response.xpath("//a[contains(@class,'property-staff-link')]/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Dixon Kestles")
        
        landlord_phone = response.xpath("//i[contains(@class,'phone')]//parent::a//@href").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split(":")[1]
            item_loader.add_value("landlord_phone", landlord_phone)
        
        yield item_loader.load_item()