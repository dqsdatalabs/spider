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
    name = 'greensquarewaterloo_ljhooker_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Greensquarewaterloo_Ljhooker_Com_PySpider_australia'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://greenwithgoldengrove.ljhooker.com.au/search/unit_apartment-for-rent/page-1?surrounding=true",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://greenwithgoldengrove.ljhooker.com.au/search/house+townhouse+duplex_semi_detached+penthouse+terrace-for-rent/page-1?surrounding=True&liveability=False",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://greenwithgoldengrove.ljhooker.com.au/search/studio-for-rent/page-1?surrounding=True&liveability=False",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'property-content')]/div[@onclick]//a[contains(@class,'property-sticker')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        night = response.xpath("//div[@class='property-heading']/h2/text()").extract_first()
        if "night" in night.lower():
            return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])           
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//div[contains(@class,'code')]//text()").get()
        if external_id:
            external_id = external_id.split("ID")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//script[contains(.,'address')]//text()").get()
        if city:
            city = city.split('addressLocality":"')[1].split('"')[0]
            item_loader.add_value("city", city)

        zipcode = response.xpath("//script[contains(.,'address')]//text()").get()
        if zipcode:
            zipcode_region = zipcode.split('addressRegion":"')[1].split('"')[0]
            zipcode_postal = zipcode.split('postalCode":"')[1].split('"')[0]
            item_loader.add_value("zipcode", zipcode_region + " " + zipcode_postal)

        square_meters = response.xpath("//strong[contains(.,'Land')]//parent::li/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].split(",")[0].strip()
            item_loader.add_value("square_meters", square_meters)

        rent = response.xpath("//div[contains(@class,'property-heading')]//h2//text()").get()
        if rent:
            if "$" in rent:
                rent = rent.strip().split("$")[-1].split(" ")[0].strip()
                
            item_loader.add_value("rent",int(float(rent) * 4))
        item_loader.add_value("currency", "AUD")

        desc = " ".join(response.xpath("//div[contains(@class,'property-text')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(@class,'bed')]/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(@class,'bathroom')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'slidethumb')]//@data-cycle-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//strong[contains(.,'Available')]//parent::li/text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//span[contains(@class,'car')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//strong[contains(.,'Furniture')]//parent::li/text()[contains(.,'Yes')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        swimming_pool = response.xpath("//li[contains(.,'In-Ground Pool')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
            
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        latitude_longitude = response.xpath("//script[contains(.,'latitude')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude":')[1].split(',')[0]
            longitude = latitude_longitude.split('longitude":')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@class,'agent-name')]//h3/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "LJ Hooker Greenwith")
        
        landlord_phone = response.xpath("//span[contains(@class,'agent-mobile')]//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "(08) 8289 9756")

        item_loader.add_value("landlord_email", "rentals.greenwith@ljh.com.au")
        yield item_loader.load_item()