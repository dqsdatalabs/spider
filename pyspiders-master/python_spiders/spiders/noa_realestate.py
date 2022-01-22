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
import dateparser

class MySpider(Spider):
    name = 'noa_realestate'    
    execution_type='testing'
    country = 'belgium'
    locale = 'en' 

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.noa-re.be/en/for-rent/house/apartment/",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.noa-re.be/en/for-rent/house/",
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
        
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[@class='image-container' and not(contains(@href,'stay-informed'))]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("/pagina")[0] + f"/pagina-{page}/"
            yield Request(p_url, callback=self.parse, meta={'property_type': response.meta.get('property_type'), "page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        if response.xpath("//figure[@id='property-image']/figcaption/text()").get() and "under offer" in response.xpath("//figure[@id='property-image']/figcaption/text()").get().lower():
            return
        property_type = response.meta.get('property_type')
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            if "Studio" in title:
                property_type= "studio"
        item_loader.add_value("property_type",property_type)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Noa_Realestate_PySpider_france")

        external_id = response.xpath("//span[@class='reference']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())

        address = " ".join(response.xpath("//div[@id='address']/address//text()").getall()).strip()
        if address:
            address = address.strip().replace("\t","").replace("\n","").replace("Address on request","").strip()
            item_loader.add_value("address", address.replace('\xa0', ''))        
            item_loader.add_value("city", address.split(" ")[-1].strip())        
            item_loader.add_value("zipcode", address.split(" ")[-2].strip()) 
            
        

        description = " ".join(response.xpath("//div[@itemprop='description']//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        
        square_meters = response.xpath("//dt[contains(.,'Living area')]/following-sibling::dd/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.split('m')[0].strip()))))

        room_count = response.xpath("//div[@class='toolbar']//i[contains(@class,' room')]/../text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        elif not room_count:
            room_count = response.xpath("substring-after(//dt[contains(.,'Bedroom')]//text(),'room')").get()
            if room_count.strip().isdigit():
                item_loader.add_value("room_count", room_count.strip())        
        
        bathroom_count = response.xpath("//div[@class='toolbar']//i[contains(@class,'bathroom')]/../text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//span[@itemprop='price']/text()").get()
        if rent:
            rent = rent.strip().replace('.', '').replace('\xa0', '')
            if rent.isnumeric():
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", 'EUR')
        
        available_date = response.xpath("//dt[contains(.,'Available')]/following-sibling::dd/span/text()[last()]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %B %Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//div[@id='property-images']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        latitude = response.xpath("//script[contains(.,'var lat')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('lat =')[1].split(';')[0].strip())
            item_loader.add_value("longitude", latitude.split('lng =')[1].split(';')[0].strip())
        
        energy_label = response.xpath("//div[@id='epc-levels']/div[contains(@style,'margin-top')]/span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        floor = response.xpath("//dt[contains(.,'Floor')]/following-sibling::dd/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        utilities = response.xpath("//dt[contains(.,'Costs')]/following-sibling::dd/span/text()[1]").get()
        if utilities:
            item_loader.add_value("utilities", str(int(float(utilities.lower().split('€')[-1].split('month')[0].strip().replace(' ', '').replace('\xa0', '')))))

        parking = response.xpath("//dt[contains(.,'Parking inside') or contains(.,'Garage')]/following-sibling::dd/span/text()").get()
        if parking:            
            if parking.strip().lower() == 'no':
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        furnished = response.xpath("//dt[contains(.,'Furnished')]/following-sibling::dd/span/text()").get()
        if furnished:
            if furnished.strip().lower() == 'yes':
                item_loader.add_value("furnished", True)
            elif furnished.strip().lower() == 'no':
                item_loader.add_value("furnished", False)

        elevator = response.xpath("//dt[contains(.,'Elevator')]/following-sibling::dd/span/text()").get()
        if elevator:
            if elevator.strip().lower() == 'yes':
                item_loader.add_value("elevator", True)
            elif elevator.strip().lower() == 'no':
                item_loader.add_value("elevator", False)
        
        terrace = response.xpath("//dt[contains(.,'Terraces')]/following-sibling::dd/span/text()").get()
        if terrace:
            if terrace.strip().isnumeric():
                if int(terrace.strip()) > 0:
                    item_loader.add_value("terrace", True)

        swimming_pool = response.xpath("//dt[contains(.,'Pool')]/following-sibling::dd/span/text()").get()
        if swimming_pool:
            if swimming_pool.strip().lower() == 'yes':
                item_loader.add_value("swimming_pool", True)
            elif swimming_pool.strip().lower() == 'no':
                item_loader.add_value("swimming_pool", False)
        
        item_loader.add_value("landlord_name", 'NOA real estate')
        item_loader.add_value("landlord_phone", '02 344 44 22')
        item_loader.add_value("landlord_email", 'info@noa-re.be')

        yield item_loader.load_item()
