# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json, re
from datetime import datetime
from datetime import date
import dateparser

class MySpider(Spider):
    name = 'nswproperties_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source = "Nswproperties_Co_PySpider_united_kingdom"
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://nswproperties.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=Apartment&ajax_radius=",
                    "https://nswproperties.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=Flat&ajax_radius=",
                    "https://nswproperties.co.uk/search/?instruction_type=Letting&extra_2=1&extra_1=%21Overseas&orderby=price+desc&department=Residential&ajax_polygon=&property_type=Apartment&ajax_radius="
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://nswproperties.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=Detached+Bungalow&ajax_radius=",
                    "https://nswproperties.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=Detached+House&ajax_radius=",
                    "https://nswproperties.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=End+Terraced+House&ajax_radius=",
                    "https://nswproperties.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=Mid+Terraced+House&ajax_radius=",
                    "https://nswproperties.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=Semi-Detached+House&ajax_radius=",
                    "https://nswproperties.co.uk/search/?instruction_type=Letting&extra_2=1&extra_1=%21Overseas&orderby=price+desc&department=Residential&ajax_polygon=&property_type=End+Terraced+House&ajax_radius=",
                    "https://nswproperties.co.uk/search/?instruction_type=Letting&extra_2=1&extra_1=%21Overseas&orderby=price+desc&department=Residential&ajax_polygon=&property_type=Semi-Detached+House&ajax_radius=",
                    "https://nswproperties.co.uk/search/?instruction_type=Letting&extra_2=1&extra_1=%21Overseas&orderby=price+desc&department=Residential&ajax_polygon=&property_type=Detached+House&ajax_radius=",
                    "https://nswproperties.co.uk/search/?instruction_type=Letting&extra_2=1&extra_1=%21Overseas&orderby=price+desc&department=Residential&ajax_polygon=&property_type=Mid+Terraced+House&ajax_radius="

                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//div[contains(@class,'property')]//a[contains(.,'DETAILS')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))    
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        external_id = response.url.split('property-details/')[-1].split('/')[0]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//span[@itemprop='name']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(',')[-1].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//span[@itemprop='description']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//br/following-sibling::text()[contains(.,'Bedrooms:')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(':')[-1].strip())
        
        bathroom_count = response.xpath("//div[@id='property-long-description']/ul/li[contains(.,'Bathroom')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", '1')

        rent = response.xpath("//span[@itemprop='price']/text()").get()
        if rent:
            if "pw" in rent.lower():
                rent = rent.split('£')[-1].lower().split('p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))*4))
            else:
                rent = rent.split('£')[-1].lower().split('pcm')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))
        item_loader.add_value("currency", 'GBP')
        
        available_date = response.xpath("//br/following-sibling::text()[contains(.,'Available:')]").get()
        if available_date:
            if "Immediately" in available_date:
                available_date = datetime.now().strftime("%Y-%m-%d")
                item_loader.add_value("available_date", available_date)
            date_parsed = dateparser.parse(available_date.split(':')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//br/following-sibling::text()[contains(.,'Deposit:')]").get()
        if deposit:
            item_loader.add_value("deposit", str(int(float(deposit.split('£')[-1].split('PC')[0].strip().replace(',', '')))))
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='property-floorplans']/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'ShowMap')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('&q=')[1].split('%2C')[0].strip())
            item_loader.add_value("longitude", latitude.split('&q=')[1].split('%2C')[1].split('"')[0].strip())

        pets_allowed = response.xpath("//br/following-sibling::text()[contains(.,'No Pets')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)
        else:
            pets_allowed = response.xpath("//br/following-sibling::text()[contains(.,'Pets')]").get()
            if pets_allowed:
                item_loader.add_value("pets_allowed", True)
        
        parking = response.xpath("//div[@id='property-long-description']/ul/li[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        terrace = response.xpath("//div[@id='property-long-description']/ul/li[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "NSW Properties")
        item_loader.add_value("landlord_phone", "01695 581260")
        item_loader.add_value("landlord_email", "info@nswproperties.co.uk")

        yield item_loader.load_item()