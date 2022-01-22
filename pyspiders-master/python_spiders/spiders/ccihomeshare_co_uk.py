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
    name = 'ccihomeshare_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": "http://ccihomeshare.co.uk/rooms/page/1/?purpose=rent&location&min_price&max_price&wre-orderby",
                "property_type": "room"
            },
        ]  # LEVEL 1
         
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//div[contains(@class,'property-item')]//div[contains(@class,'image')]//@href[contains(.,'listing')]").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})
            seen=True
        
        if page ==2 or seen:        
            f_url = f"http://ccihomeshare.co.uk/rooms/page/{page}/?purpose=rent&location&min_price&max_price&wre-orderby"
            yield Request(f_url, callback=self.parse, meta={"page": page+1, 'property_type':response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if not "listing" in response.url:
            return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", "Ccihomeshare_Co_PySpider_united_kingdom")

        external_id = response.xpath("//li[contains(.,'Property ID')]//parent::li//span//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        externalidcheck=item_loader.get_output_value("external_id")
        if not externalidcheck:
            externalid=response.xpath("//link[@rel='shortlink']/@href").get()
            if externalid:
                item_loader.add_value("external_id",externalid.split("=")[-1])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//p[contains(@class,'property--location')]//text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//span[contains(.,'City')]//parent::li/text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        zipcode = response.xpath("//span[contains(.,'Post code')]//parent::li/text()").get()
        if zipcode:
            zipcode = zipcode.strip()
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//p[contains(@class,'property--price')]//text()").get()
        if rent:
            rent = rent.replace("£","").replace("pcm","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'property--details')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//h5[contains(.,'Bed')]//following-sibling::p//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            if room_count != "0":
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//h5[contains(.,'Bath')]//following-sibling::p//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            if bathroom_count != "0":
                item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//ul[contains(@id,'image-gallery')]//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//li[contains(.,'Available from')]//parent::li//span//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//h5[contains(.,'Parking')]//following-sibling::p//text()").get()
        if parking:
            item_loader.add_value("parking", True)
            
        dishwasher = response.xpath("//div[contains(@class,'feature-item')]//text()[contains(.,'Dish Washer')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        latitude_longitude = response.xpath("//script[contains(.,'lat\"')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat":"')[1].split('"')[0]
            longitude = latitude_longitude.split('lng":"')[1].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@class,'agent--info')]//h5//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//div[contains(@class,'agent--contact')]//i[contains(@class,'envelope')]//parent::li//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        
        landlord_phone = response.xpath("//div[contains(@class,'agent--contact')]//i[contains(@class,'phone')]//following-sibling::a//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()