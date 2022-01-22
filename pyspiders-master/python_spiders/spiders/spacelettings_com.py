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
from datetime import datetime
from datetime import date

class MySpider(Spider):
    name = 'spacelettings_com'    
    start_urls = ["https://www.spacelettings.com/Search?listingType=6&statusids=1&obc=Price&obd=Descending&obc=Price&obd=Descending&category=1&areainformation=&minprice=&maxprice=&radius=&bedrooms=&cipea=1&perpage=36000"]
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='searchResultBoxBg']/figure/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//i[@class='i-next']/../@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        desc = "".join(response.xpath("//div[@class='descriptionText']/text()").getall())
        if desc and ("apartment" in desc.lower() or "flat" in desc.lower() or "maisonette" in desc.lower()):
            item_loader.add_value("property_type", "apartment")
        elif desc and "house" in desc.lower():
             item_loader.add_value("property_type", "house")
        elif desc and "studio" in desc.lower():
             item_loader.add_value("property_type", "studio")
        else:
            return

        item_loader.add_value("external_source", "Spacelettings_PySpider_"+ self.country + "_" + self.locale)

        address = response.xpath("//script[@id='movetoFDTitle']/text()").get()
        if address:
            address = address.replace('<h3>', '').replace('</h3>', '').strip()
            city = address.split(',')[-2].strip()
            zipcode = address.split(',')[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='descriptionText']//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

            if 'Available ' in description:
                available_date = description.split('Available ')[1].split('.')[0].split('from')[-1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"], languages=['en'])
                if date_parsed:
                    compare_today = datetime.combine(date.today(), datetime.min.time()) > date_parsed
                    if compare_today:
                        date_parsed = date_parsed.replace(year = date_parsed.year + 1)
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            
            if 'Ref' in description:
                external_id = description.split('Ref')[1].strip().strip(':').strip().split('.')[0].strip().split(' ')[0].strip()
                item_loader.add_value("external_id", external_id)
            
            if 'EPC ' in description:
                energy_label = description.split('EPC ')[1].split('.')[0].strip().split(' ')[-1].strip()
                if energy_label in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                    item_loader.add_value("energy_label", energy_label)

            if 'unfurnished' in description.lower():
                item_loader.add_value("furnished", False)
            elif 'fully furnished' in description.lower():
                item_loader.add_value("furnished", True)
        
        room_count = response.xpath("//span[contains(.,'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.lower().split('bedroom')[0].strip())

        bathroom_count = response.xpath("//span[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.lower().split('bathroom')[0].strip())

        rent = response.xpath("//div[contains(@class,'FDPrice')]//h4/div[1]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split('£')[-1].strip().split(' ')[0].strip().replace(',', ''))
            item_loader.add_value("currency", 'GBP')
        
        images = [x for x in response.xpath("//div[@class='FDSlider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor = response.xpath("//span[contains(.,'Floor Level')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.lower().split('floor')[0].strip())
        
        parking = response.xpath("//span[contains(.,'Parking Space')]/text()").get()
        if parking:
            parking = int(parking.lower().split('parking')[0].strip())
            if parking > 0:
                item_loader.add_value("parking", True)
        
        item_loader.add_value("landlord_name", 'ST ALBANS')
        item_loader.add_value("landlord_phone", '01727 862381')
        item_loader.add_value("landlord_email", 'info@spacelettings.com')

        yield item_loader.load_item()
