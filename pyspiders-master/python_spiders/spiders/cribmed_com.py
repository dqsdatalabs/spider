# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector 
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'cribmed_com'
    external_source = "Cribmed_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it' 
    start_urls = ['https://cribmed.com/advanced-search-2/?property_city=&property_area=&check_in=&property_bedrooms=&price_low=0&price_max=20000&submit=Search']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'property_listing')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://cribmed.com/advanced-search-2/page/{page}/?property_city&property_area&check_in&property_bedrooms&price_low=0&price_max=20000&submit=Search"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)


        external_id = response.xpath("//div[contains(@class,'listing_detail list_detail_prop_id col-md-6')]//span[contains(.,'Listing ID:')]//following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title.replace("\u2013",""))
        
        desc = "".join(response.xpath("//div[@itemprop='description']//text()").getall())
        if desc:
            item_loader.add_value("description", desc)
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            prop_type = response.xpath("//div[contains(@class,'category_details_wrapper')]/a[2]/text()").get()
            if get_p_type_string(prop_type):
                item_loader.add_value("property_type", get_p_type_string(prop_type))
            else:
                return
        address = "".join(response.xpath("//div[contains(@class,'schema_div_noshow')]//text()").get())
        if address:
            item_loader.add_value("address",address)

        city = "".join(response.xpath("//div[contains(@class,'listing_main_img_location')]//a[contains(@rel,'tag')]//text()").get())
        if city:
            city=city.strip().split(",")[:1]
            item_loader.add_value("city",city)

        bathroom_count = response.xpath("//span[contains(.,'Bath')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(" ")[0])
        room_count=response.xpath("//span[contains(.,'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("Bed")[0])

        square = response.xpath("//div[contains(@class,'listing_detail list_detail_prop_size col-md-6')]//span[contains(.,'Listing Size:')]//following-sibling::text()").get()
        if square:
            square=square.split("m")
            item_loader.add_value("square_meters", square)

        rent ="".join(response.xpath("//div[contains(@id,'listing_price_text')]//span[contains(.,'from')]//following-sibling::text()[contains(.,'€')]").getall())
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit ="".join(response.xpath("//div[contains(@id,'listing_price_text')]//span[contains(.,'from')]//following-sibling::text()[contains(.,'€')]").getall())
        if rent:
            item_loader.add_value("deposit", deposit)

        images = [response.urljoin(x)for x in response.xpath("//img[contains(@itemprop,'image')]//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        terrace = response.xpath("//div[contains(@class,'panel-body panel-body-border')]//div[contains(@class,'listing_detail col-md-6')]//text()[contains(.,'Terrace Private')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        balcony = response.xpath("//div[contains(@class,'panel-body panel-body-border')]//div[contains(@class,'listing_detail col-md-6')]//text()[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        latitude= response.xpath("//div[@id='google_map_on_list']/@data-cur_lat").extract()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude= response.xpath("//div[@id='google_map_on_list']/@data-cur_long").extract()
        if longitude:
            item_loader.add_value("longitude",longitude)
       
        item_loader.add_value("landlord_phone", "3274766480")
        item_loader.add_value("landlord_email", "info@cribmed.com")
        item_loader.add_value("landlord_name", "Cribmed")
            
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartamento" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "casa" in p_type_string.lower() or "huis" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None
      