# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n

class MySpider(Spider):
    name = 'yorkestates_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["http://www.yorkestates.co.uk/lettings/"]
    external_source="Yorkestates_Co_PySpider_united_kingdom"

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='image']"):
            status = item.xpath("./p/text()").get()
            if status and "let" in status.lower().strip():
                continue
            follow_url = response.urljoin(item.xpath("./div/a/@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[contains(@class,'btn--next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        full_text = " ".join(response.xpath("//div[@class='print-page']/p/text()").getall())
        if get_p_type_string(full_text):
            item_loader.add_value("property_type", get_p_type_string(full_text))
        else:
            return

        item_loader.add_value("external_source", self.external_source)

        address = " ".join(response.xpath("//div[@id='masthead']//div[@class='text']//text()").getall()).strip()
        if address:
            item_loader.add_value("address", address.strip().replace('\t', '').replace('\n', ''))

        zipcode = response.xpath("//div[@id='masthead']//h2/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//h3[contains(.,'Property Details')]/following-sibling::p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        else:
            description = ""

        square_meters = response.xpath("//text()[contains(.,'sq ft')]").get()
        if square_meters:
            square_meters = square_meters.split('sq ft')[0].strip().split(' ')[-1].strip().replace(',', '')
            item_loader.add_value("square_meters", str(int(float(square_meters) * 0.09290304)))

        room_count = response.xpath("//li[contains(.,'Bedroom') or contains(.,'bedroom')]/text()").get()
        if room_count:
            try:
                item_loader.add_value("room_count", w2n.word_to_num(room_count.lower().split(' ')[0].strip()))
            except:
                pass
        elif response.xpath("//li[contains(.,'Studio') or contains(.,'studio')]/text()").get():
            item_loader.add_value("room_count", 1)
        elif "accommodation" in description.lower():
            desc_text = description.lower().split("accommodation")[1].split("bedroom")[0].split(",")[1].strip().split(" ")[0]
            try:
                item_loader.add_value("room_count", w2n.word_to_num(desc_text))
            except:
                pass 


        
        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            try:
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count.lower().split('bathroom')[0].strip()))
            except:
                pass

        rent = response.xpath("//h3[@class='asking-price']/following-sibling::p[1]/text()").get()
        if rent:
            rent = rent.split('£')[-1].lower().split('p')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent)) * 4))
            item_loader.add_value("currency", 'GBP')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = " ".join(response.xpath("//text()[contains(.,'AVAILABLE FROM THE')]/..//text()").getall()).strip()
        if available_date:
            date_parsed = dateparser.parse(available_date.split('AVAILABLE FROM THE')[-1].split('.')[0].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x.strip()) for x in response.xpath("//div[@id='slider']/@data-images").get().split(',')]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//h3[contains(.,'Floor Plan')]/..//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//div[@id='map']/@data-geolocation").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split(',')[1].strip())
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony') or contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//text()[contains(.,'AVAILABLE UNFURNISHED')]").get()
        if furnished:
            item_loader.add_value("furnished", False)
        else:
            furnished = response.xpath("//text()[contains(.,'AVAILABLE FURNISHED')]").get()
            if furnished:
                item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'Lift') or contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//li[contains(.,'Terrace') or contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "YORK ESTATES")
        item_loader.add_value("landlord_phone", "+44 (0)20 7724 0335")
        item_loader.add_value("landlord_email", "enquiries@yorkestates.co.uk")

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
