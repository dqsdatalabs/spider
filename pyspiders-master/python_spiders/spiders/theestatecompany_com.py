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
    name = 'theestatecompany_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.theestatecompany.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Apartment", "property_type": "apartment"},
            {"url": "https://www.theestatecompany.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Flat", "property_type": "apartment"},
	        {"url": "https://www.theestatecompany.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Detached+House", "property_type": "house"},
            {"url": "https://www.theestatecompany.com/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Semi-Detached+House", "property_type": "house"},   
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url": url.get('url')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@id='search-results']//div[contains(@class,'property')]//h4/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta.get("base_url")
            url = base_url.replace(f"search/?",f"search/{page}.html?")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type":property_type, "base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//title/text()").get()
        if title: 
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))

        item_loader.add_value("external_source", "Theestatecompany_PySpider_united_kingdom")

        external_id = response.url.split('property-details/')[-1].split('/')[0]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//span[@id='container--address']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            if address.strip().count(',') >= 1: 
                city = address.split(',')[-1].strip()
                if "Road" not in city and "Terrace" not in city:
                    item_loader.add_value("city", "London")
        
        description = " ".join(response.xpath("//div[@id='overview']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.replace('\xa0', '').strip()))

        square_meters = response.xpath("//li[contains(.,'sq ft')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.split('sq')[0].strip()) * 0.09290304)))

        room_count = response.xpath("//*[local-name()='svg' and @class='icon-bedrooms']/following-sibling::strong[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//*[local-name()='svg' and @class='icon-bathrooms']/following-sibling::strong[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//span[@id='container--price']/text()").get()
        if rent:
            rent = rent.split('£')[-1].lower().split('p')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'GBP')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split('Available')[-1].strip().replace('Immediately', 'now'), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='modal--property-carousel']//div[contains(@class,'swiper-slide')]/@data-background").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floorplan']/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('"latitude": "')[1].split('"')[0].strip())
            item_loader.add_value("longitude", latitude.split('"longitude": "')[1].split('"')[0].strip())
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony') or contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]").get()
        if furnished:
            item_loader.add_value("furnished", False)
        elif response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]").get(): item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'Lift') or contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//li[contains(.,'Terrace') or contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "The Estate Company")
        item_loader.add_value("landlord_phone", "020 7372 5000")
        item_loader.add_value("landlord_email", "enquiries@theestatecompany.com")

        yield item_loader.load_item()