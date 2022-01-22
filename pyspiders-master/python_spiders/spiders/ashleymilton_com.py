# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
from datetime import date
import dateparser

class MySpider(Spider):
    name = 'ashleymilton_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.ashleymilton.com/properties-to-let?start=0"]

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='eapow-property-thumb-holder']"):
            status = item.xpath("./div[@class='eapow-bannertopright']/img/@src").get()
            if status and ("let" in status.lower().strip() or "underoffer" in status.lower().strip()):
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[@title='Next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        full_text = " ".join(response.xpath("//div[contains(@class,'eapow-desc-wrapper')]/p/text()").getall())
        if get_p_type_string(full_text):
            item_loader.add_value("property_type", get_p_type_string(full_text))
        else:
            return

        item_loader.add_value("external_source", "Ashleymilton_PySpider_united_kingdom")

        external_id = response.xpath("//b[contains(.,'Ref')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())

        address = " ".join(response.xpath("//div[@id='propdescription']//address//text()").getall()).strip()
        if address:
            item_loader.add_value("address", address.strip())
        
        city_zipcode = response.xpath("//div[@id='propdescription']//address/br/following-sibling::text()").get()
        if city_zipcode:
            item_loader.add_value("zipcode", " ".join(city_zipcode.strip().split(' ')[1:]).replace("Vale","").strip())
            item_loader.add_value("city", city_zipcode.strip().split(' ')[0].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[contains(@class,'desc')]/p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//li[contains(.,'sq ') or contains(.,'Sq ')]/text()").get()
        if square_meters:
            if 'sq m' in square_meters.lower():
                item_loader.add_value("square_meters", "".join(filter(str.isnumeric, square_meters.split('sq ')[0].strip())))
            elif 'sq ft' in square_meters.lower():
                square_meters = "".join(filter(str.isnumeric, square_meters.split('sq ')[0].strip()))
                if square_meters:
                    item_loader.add_value("square_meters", str(int(float(square_meters) * 0.09290304)))

        room_count = response.xpath("//img[contains(@src,'bedroom')]/following-sibling::strong[1]/text()").get()
        if room_count:
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//img[contains(@src,'bathroom')]/following-sibling::strong[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//small/text()").get()
        if rent:
            rent = rent.split('£')[-1].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent)) * 4))
            item_loader.add_value("currency", 'GBP')


        available_date = response.xpath("//li[contains(.,'Available')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().split('available')[-1].split('from')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        else:
            available_date = response.xpath("//li[contains(.,'AVAILABLE')]/text()").get()
            if available_date:
                if "now" in available_date.lower():
                    item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
                date_parsed = dateparser.parse(available_date.lower().split('available')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
                today = datetime.combine(date.today(), datetime.min.time())
                if date_parsed:
                    result = today > date_parsed
                    if result == True:
                        date_parsed = date_parsed.replace(year = today.year + 1)
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='eapow-prop-gallery']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='eapowfloorplanplug']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'lat:')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('lat: "')[1].split('"')[0].strip())
            item_loader.add_value("longitude", latitude.split('lon: "')[1].split('"')[0].strip())

        energy_label = response.xpath("//text()[contains(.,'EPC rating')]").get()
        if energy_label:
            energy_label = energy_label.split('EPC rating')[1].split('=')[-1].strip().split(' ')[0].strip().strip('.')
            if energy_label in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label)
        
        floor = response.xpath("//li[contains(.,'Floor')]").get()
        if floor:
            floor = floor.lower().split('floor')[0].strip()
            item_loader.add_value("floor", floor)
        
        parking = response.xpath("//li[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony') or contains(.,'BALCONY')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'Unfurnished')]").get()
        if furnished:   
            item_loader.add_value("furnished", False)
        else:
            furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]").get()
            if furnished:
                item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'Lift') or contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        dishwasher = response.xpath("//li[contains(.,'dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        item_loader.add_value("landlord_name", "Ashley Milton Property Agents")
        item_loader.add_value("landlord_phone", "020 7286 6565")
        item_loader.add_value("landlord_email", "property@ashleymilton.com")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
