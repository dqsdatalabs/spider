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
    name = 'bvwonen_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    external_source = 'Bvwonen_PySpider_netherlands'


    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.bvwonen.nl/aanbod/huurwoningen?of=eyJwcmljZV9taW4iOjAsInByaWNlX21heCI6MjI1MDAwMDAwMCwic29ydHMiOiIxIiwicGxhY2UiOjB9&offset_62=1&",
                ],
                "property_type": "apartment",
            },
            {
                "url" : [
                    "https://www.bvwonen.nl/aanbod/huurwoningen?of=eyJwcmljZV9taW4iOjAsInByaWNlX21heCI6MjI1MDAwMDAwMCwic29ydHMiOiIyIiwicGxhY2UiOjB9&offset_62=1&",
                ],
                "property_type": "house",
            },
            {
                "url" : [
                    "https://www.bvwonen.nl/aanbod/huurwoningen?of=eyJwcmljZV9taW4iOjAsInByaWNlX21heCI6MjI1MDAwMDAwMCwic29ydHMiOiI0IiwicGxhY2UiOjB9&offset_62=1&",
                ],
                "property_type": "studio",
            },
            {
                "url" : [
                    "https://www.bvwonen.nl/aanbod/huurwoningen?of=eyJwcmljZV9taW4iOjAsInByaWNlX21heCI6MjI1MDAwMDAwMCwic29ydHMiOiIzIiwicGxhY2UiOjB9&offset_62=1&",
                ],
                "property_type": "room",
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='col-1 thumb']"):
            if item.xpath(".//div[@class='status-label label-verhuurd']/text()[contains(.,'Verhuurd')]").get():
                continue
            
            follow_url = item.xpath(".//a/@href").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:            
            p_url = response.url.replace(f"offset_62={page-1}&", f"offset_62={page}&")
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type": response.meta.get('property_type')})
                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        title = response.xpath("//div[@class='object_block col-md-8 left']/h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        external_id = response.xpath("//div[@data-row='dtg_woning-code']/div[2]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        rent = response.xpath("//div[@data-row='dtg_price']/div[2]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace("€",""))
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//div[@data-row='dtg_rooms']/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[@data-row='dtg_bedrooms']/div[2]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)

        square_meters = response.xpath("//div[@data-row='dtg_area']/div[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.replace("m", ""))
        
        deposit = response.xpath("//div[@data-row='dtg_borg']/div[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("€",""))

        available_date = response.xpath("//div[@data-row='dtg_beschikbaarheidsdatum']/div[2]/text()").get()
        if available_date and ("per direct" not in available_date):
            date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        city = response.url
        if city:
            city = city.split("object/")[1].split("/")[0]
            item_loader.add_value("city", city)
            item_loader.add_value("address", city)
        
        description = " ".join(response.xpath("//div[@class='col-1']/p/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [response.urljoin("https://www.bvwonen.nl"+ x) for x in response.xpath("//div[@id='content']//img[@class='img-responsive']/@src").getall()]
        if images:
            item_loader.add_value("images", images)  
        
        item_loader.add_value("landlord_name", "BVwonen B.V.")
        item_loader.add_value("landlord_phone", "013-5353205")
        item_loader.add_value("landlord_email", "info@bvwonen.nl")
        
        yield item_loader.load_item()