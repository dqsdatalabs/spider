# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy

class MySpider(Spider):
    name = 'bassets_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.bassets.co.uk/property-search/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=22&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&address_keyword=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.bassets.co.uk/property-search/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=9&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&address_keyword=",
                    "https://www.bassets.co.uk/property-search/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=18&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&address_keyword=",
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
        for item in response.xpath("//div[@class='thumbnail']/a"):
            status = item.xpath("./div/div[@class='flag']/text()").get()
            if status and "let agreed" in status.strip().lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Bassets_Co_PySpider_united_kingdom")

        comment_line = str(response.body).split('<div class="property_meta">')[1].split('</div>')[0].strip().split('<!--')[1].split('-->')[0].strip()
        meta_data = scrapy.Selector(text=comment_line, type="html")

        if meta_data:

            external_id = meta_data.xpath("//li[@class='ref']/text()").get()
            if external_id:
                item_loader.add_value("external_id", external_id.split(':')[-1].strip())

            room_count = meta_data.xpath("//li[@class='bedrooms']/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(':')[-1].strip())

            furnished = meta_data.xpath("//li[@class='furnished']/text()").get()
            if furnished:
                if furnished.split(':')[-1].strip().lower() == 'furnished':
                    item_loader.add_value("furnished", True)
                elif furnished.split(':')[-1].strip().lower() == 'unfurnished':
                    item_loader.add_value("furnished", False)

            deposit = meta_data.xpath("//li[@class='deposit']/text()").get()
            if deposit:
                item_loader.add_value("deposit", "".join(filter(str.isnumeric, deposit.split(':')[-1].strip())))

            from datetime import datetime
            from datetime import date
            import dateparser
            available_date = meta_data.xpath("//li[@class='available']/text()").get()
            if available_date:
                date_parsed = dateparser.parse(available_date.split(':')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
                today = datetime.combine(date.today(), datetime.min.time())
                if date_parsed:
                    result = today > date_parsed
                    if result == True:
                        date_parsed = date_parsed.replace(year = today.year + 1)
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        address = response.xpath("//h1/text()").get()
        if address:
            city = address.split(",")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='description']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        rent = response.xpath("//div[@class='price']/h2/text()").get()
        if rent:
            rent = rent.split('£')[-1].lower().split('p')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='slides']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//li[@class='action-floorplans']/a/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('LatLng(')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip())
        
        energy_label = response.xpath("normalize-space(//text()[contains(.,'EPC rating:')])").get()
        if energy_label:
            if energy_label.split(':')[-1].strip().strip('”').upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.split(':')[-1].strip().strip('"').upper().replace('\u201d', ''))

        pets_allowed = response.xpath("//text()[contains(.,'Pets:')]").get()
        if pets_allowed:
            if 'considered' in pets_allowed.lower():
                item_loader.add_value("pets_allowed", True)
        
        parking = response.xpath("//li[contains(text(),'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        washing_machine = response.xpath("//text()[contains(.,'Appliances included:')]").get()
        if washing_machine:
            if 'washing machine' in washing_machine.lower():
                item_loader.add_value("washing_machine", True)

        item_loader.add_value("landlord_name", "Bassets Lettings")
        item_loader.add_value("landlord_phone", "01722 820580")
        item_loader.add_value("landlord_email", "lettings@bassets.co.uk")
      
        yield item_loader.load_item()