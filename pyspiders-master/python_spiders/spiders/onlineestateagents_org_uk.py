# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'onlineestateagents_org_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.onlineestateagents.org.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Apartment", "property_type": "apartment"},
            {"url": "https://www.onlineestateagents.org.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Studio", "property_type": "studio"},
            {"url": "https://www.onlineestateagents.org.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Maisonette", "property_type": "apartment"},
	        {"url": "https://www.onlineestateagents.org.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Detached+Bungalow", "property_type": "house"},
            {"url": "https://www.onlineestateagents.org.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Detached", "property_type": "house"},
            {"url": "https://www.onlineestateagents.org.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Bungalow", "property_type": "house"},
            {"url": "https://www.onlineestateagents.org.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=End+of+Terrace", "property_type": "house"},
            {"url": "https://www.onlineestateagents.org.uk/search/?showstc=off&showsold=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Semi-Detached", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//div[contains(@class,'property-thumb-height')]/div[contains(@class,'property-element')]//a[contains(.,'Details')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Onlineestateagents_O_PySpider_united_kingdom")

        external_id = response.url.split('property-details/')[-1].split('/')[0].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip())
            item_loader.add_value("city", address.split(',')[-2].strip())

        description = " ".join(response.xpath("//div[@id='property-long-description']/div[@class='row']/div[1]//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//*[local-name() = 'svg' and @class='icon--bedrooms']/following-sibling::strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//*[local-name() = 'svg' and @class='icon--bathrooms']/following-sibling::strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//h2/text()").get()
        if rent:
            rent = rent.split('£')[-1].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'GBP')

        deposit = response.xpath("//br/following-sibling::text()[contains(.,'Deposit')]").get()
        if deposit:
            deposit = deposit.split('£')[-1].strip().replace(',', '').replace('\xa0', '')
            if deposit.replace('.', '').isnumeric():
                item_loader.add_value("deposit", str(int(float(deposit))))

        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-carousel']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='property-floorplans']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        floor = response.xpath("//li[contains(text(),'Floor')]/text()").get()
        if floor:
            floor = "".join(filter(str.isnumeric, floor.lower().split('floor')[0]))
            if floor:
                item_loader.add_value("floor", floor)
            
        pets_allowed = response.xpath("//li[contains(.,'No pets')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)

        furnished = response.xpath("//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]").get()
        if furnished:
            item_loader.add_value("furnished", False)
        else:
            furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]").get()
            if furnished:
                item_loader.add_value("furnished", True)
        
        parking = response.xpath("//li[contains(.,'No Parking')]").get()
        if parking:
            item_loader.add_value("parking", False)
        else:
            parking = response.xpath("//li[contains(.,'Parking')]").get()
            if parking:
                item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = response.xpath("//li[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//li[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_xpath("landlord_name", "//h2[@id='profileName']/text()")
        item_loader.add_xpath("landlord_phone", "//div[@id='profile']//a[contains(@href,'tel')]/text()")
        item_loader.add_xpath("landlord_email", "//div[@id='profile']//a[contains(@href,'mail')]/text()")

        yield item_loader.load_item()