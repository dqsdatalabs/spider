# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re 
import dateparser

class MySpider(Spider):
    name = 'trechousing_nl'
    start_urls = ['https://www.trechousing.nl/woning-aanbod/'] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source="Trechousing_PySpider_netherlands_nl"
 # LEVEL 1

    # 1. FOLLOWING 
    def parse(self, response):

        for item in response.xpath("//a[@class='img-container']/@href").getall():
            follow_url=response.urljoin(item)
            square=response.xpath("//a[@class='object_data_labels']//span//span[contains(.,'m²')]/text()").get()
            room=response.xpath("//span[@class='object_label object_bed_rooms']/span[@class='number']/text()").get()
            bathroom=response.xpath("//span[@class='object_label object_bath_rooms']/span[@class='number']/text()").get()

            yield Request(follow_url, callback=self.populate_item,meta={"square":square,"room":room,"bathroom":bathroom})
        next_button = response.xpath("//a[@class='sys_paging next-page']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        dontallow=response.xpath("//td[.='Status']/following-sibling::td/text()").get()
        if dontallow and "verhuurd" in dontallow.lower():
            return 
        dontallow1=response.url
        if dontallow1 and "koop" in dontallow1:
            return 
        dontallow2=response.xpath("//td[.='Type object']/following-sibling::td/text()").get()
        if dontallow2 and "Parkeergelegenheid" in dontallow2:
            return 


        
        title = " ".join(response.xpath("//h1[@class='obj_address']//text()").extract())
        title = re.sub('\s{2,}', ' ', title)
        item_loader.add_value("title", title.strip().split(":")[-1])
        item_loader.add_value("external_link", response.url)
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("?")[0].split("/")[-1])
        square_meters=response.meta.get("square")
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(" ")[0])
        room_count=response.meta.get("room")
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.meta.get("bathroom")
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        

        price = response.xpath("//div[@class='object_price']/text()").get()
        if price:
            item_loader.add_value("rent", price.split("€")[-1].split(",")[0].replace(".",""))
        item_loader.add_value("currency", "EUR")

        property_type = response.xpath("//td[.='Type object']/following-sibling::td/text()").get()
        if get_p_type_string(property_type): 
            item_loader.add_value("property_type", get_p_type_string(property_type))
        

        images = [response.urljoin(x) for x in response.xpath("//a[@data-fancybox='listing-photos']/@href").getall()]
        if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))
        available_date=response.xpath("//td[.='Aangeboden sinds']/following-sibling::td/text()").get()
        if available_date:
            available_date = "".join(available_date.split(" ")[1:])
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

 

        desc = "".join(response.xpath("//h3[.='Beschrijving']/following-sibling::div/text()").getall())
        desc = re.sub('\s{2,}', ' ', desc)
        item_loader.add_value("description", desc)

        if desc:
            if 'terrace' in desc.lower():
                item_loader.add_value("terrace", True)
            if 'washing machine' in desc.lower():
                item_loader.add_value("washing_machine", True)
            if 'balcony' in desc.lower():
                item_loader.add_value("balcony", True)
            if 'furnished' in desc.lower():
                item_loader.add_value("furnished", True)
        furnishedcheck=item_loader.get_output_value("furnished")
        if not furnishedcheck:
            furnished=response.xpath("//tr//td[.='Gemeubileerd']/text()").get()
            if furnished:
                item_loader.add_value("furnished", True)


 
        address=item_loader.get_output_value("title")
        if address:
            item_loader.add_value("address",address)
        if address:
            item_loader.add_value("city",address.split(",")[0])
        if address:
            item_loader.add_value("zipcode",address.split(",")[-1].split(" ")[1]+" "+address.split(",")[-1].split(" ")[2])

        item_loader.add_value("landlord_phone", "070 - 763 0408")
        item_loader.add_value("landlord_email", "rentals@trechousing.nl")
        item_loader.add_value("landlord_name", "Trechousing")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "apartment" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "woonhuis" in p_type_string.lower()):
        return "house"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    else:
        return None