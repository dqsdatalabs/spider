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
    name = 'warwickhomerental_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://warwickhomerental.com/rentals"] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'room-list-item')]"):
            p_type_info = item.xpath(".//p[@class='intro-paragraph']/text()").get()
            follow_url = response.urljoin(item.xpath(".//div[@class='room-overview-title-wrapper']/a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"p_type_info":p_type_info})   

        next_page = response.xpath("//a[@class='w-pagination-next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
            )     
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        p_type_info = response.meta["p_type_info"]
        if get_p_type_string(p_type_info):
            item_loader.add_value("property_type", get_p_type_string(p_type_info))
        else:
            return

        item_loader.add_value("external_source", "Warwickhomerental_PySpider_united_kingdom")

        address = response.xpath("//h3[last()]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            if ',' in address:
                item_loader.add_value("zipcode", address.split(',')[-1].strip())
        
        item_loader.add_value("city", 'Coventry')

        title = response.xpath("//h1/text()").get()
        if title: 
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='rich-text-block w-richtext']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        if description and "*no longer available" in description.lower():
            return 

        room_count = response.xpath("//text()[contains(.,'Room type:')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split('Room type:')[1].split('bedroom')[0].strip())

            if 'bathroom' in room_count.lower():
                for i in room_count.lower().split('bathroom'):
                    if i.strip().split(' ')[-1].strip().isnumeric():
                        item_loader.add_value("bathroom_count", i.strip().split(' ')[-1].strip())
                        break

        rent = response.xpath("//div[@class='room-widget-title price']/text()").get()
        if rent:
            item_loader.add_value("rent", str(int(float(rent.replace('\xa0', ''))) * 4))
            item_loader.add_value("currency", 'GBP')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//text()[contains(.,'Available for')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split('Available for')[1].split('-')[0].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//text()[contains(.,'Deposit')]").get()
        if deposit:
            if '£' in deposit:
                item_loader.add_value("deposit", deposit.split('£')[-1].split('/')[0].strip())
            else:
                multiple = int(deposit.split('week')[0].strip().split(' ')[-1].strip())
                item_loader.add_value("deposit", str(int(rent) * multiple))
        
        images = [response.urljoin(x) for x in response.xpath("//div[contains(.,'Gallery')]//div[@role='list']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[contains(.,'Gallery')]//div[@role='list']//div[contains(.,'GF') or contains(.,'1F')]/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        utilities = "".join(response.xpath("//div[contains(@class,'price-break-down')][2]/div[2]//text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.split('£')[-1].strip())
        
        parking = response.xpath("//div[@class='text-block' and contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "Warwick Home")
        item_loader.add_value("landlord_phone", "+44 776 394 5963")
        item_loader.add_value("landlord_email", "info@warwickhomerental.com")

        map_iframe = response.xpath("//iframe[contains(@src,'google.com/maps/embed')]/@src").get()
        if map_iframe: yield Request(map_iframe, callback=self.get_map, dont_filter=True, meta={"item_loader": item_loader})

    def get_map(self, response):

        item_loader = response.meta["item_loader"]
        latitude = response.xpath("//div[@id='mapDiv']/following-sibling::script[1]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('",null,[null,null,')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('",null,[null,null,')[1].split(',')[1].split(']')[0].strip())
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    else:
        return None