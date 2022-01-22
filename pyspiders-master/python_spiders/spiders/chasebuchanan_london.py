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
    name = 'chasebuchanan_london'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://www.chasebuchanan.london/search/2.html?showstc=off&showsold=on&instruction_type=Letting&address_keyword_exact=1&nearby_results_field=ajax_polygon&ajax_polygon=&minprice=&maxprice="] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[contains(@class,'btn-primary-thumb')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True

        if page == 2 or seen:
            p_url = f"https://www.chasebuchanan.london/search/{page}.html?showstc=off&showsold=on&instruction_type=Letting&address_keyword_exact=1&nearby_results_field=ajax_polygon&ajax_polygon=&minprice=&maxprice="
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1}
            )     
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//h2[contains(.,'Details')]/../../following-sibling::text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return

        item_loader.add_value("external_source", "Chasebuchanan_PySpider_united_kingdom")

        external_id = response.url.split('property-details/')[1].split('/')[0]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//aside//h2/span[@itemprop='name']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            if "," in address:
                city = address.strip().split(",")[1].strip()
                item_loader.add_value("city", city)
      
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[contains(@class,'property-details')]/following-sibling::text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//span[@class='property-bedrooms']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//span[@class='property-bathrooms']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//span[@itemprop='price']/@content").get()
        if rent:
            item_loader.add_value("rent", str(rent).strip())
            item_loader.add_value("currency", 'GBP')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//li[contains(text(),'Available')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split('Available')[1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-thumbnails']//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//img[contains(@alt,'Floorplans')]/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'PropertyMap.render')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('(opt, ')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('(opt, ')[1].split(',')[1].split(',')[0].strip())
        
        energy_label = response.xpath("//li[contains(text(),'EPC ')]/text()").get()
        if energy_label:
            if energy_label.split('EPC')[1].strip().split(' ')[-1].upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.split('EPC')[1].strip().split(' ')[-1].upper())

        pets_allowed = response.xpath("//li[contains(text(),'Pet friendly')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)

        parking = response.xpath("//li[contains(text(),'Parking') or contains(text(),'parking') or contains(text(),'Garage') or contains(text(),'garage')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(text(),'Balcony') or contains(text(),'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(text(),'Furnished') or contains(text(),'furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(text(),'Lift') or contains(text(),'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//li[contains(text(),'Terrace') or contains(text(),'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        swimming_pool = response.xpath("//li[contains(text(),'Swimming pool') or contains(text(),'swimming pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        item_loader.add_value("landlord_name", "Chase Buchanan")


        # item_loader.add_xpath("landlord_phone", "//a[contains(@href,'Tel')]/text()")
        phone=response.xpath("//a[contains(@href,'Tel')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        else:
            phone1=item_loader.get_output_value('external_id')
            if phone1=="29513613":
                item_loader.add_value("landlord_phone","020 8948 1331")



        item_loader.add_value("landlord_email", "richmond@chasebuchanan.london")
      
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
    else:
        return None