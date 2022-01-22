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
    name = 'prd_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source = "Prd_PySpider_australia"
    start_urls = ['https://www.prd.com.au/corporate-search/?property_type_toggle=Residential&listing_type=Lease&property_type=Residential&page=1&1631022304752=&1631022313116']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//a[@class='property-card__link']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.prd.com.au/corporate-search/?property_type_toggle=Residential&listing_type=Lease&property_type=Residential&page={page}&1631022304752=&1631022313116"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//section[@class='listing__copy']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")


        item_loader.add_value("external_id", response.url.split("-")[-1].split("/")[0])
        item_loader.add_xpath("room_count", "//div[@class='listing-details__feature-group'][img[@alt='listing rooms']]/span/text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='listing-details__feature-group'][img[@alt='listing bathrooms']]/span/text()")
    
        rent = response.xpath("//h2[@class='listing-details__price']/span/text()").get()
        if rent:
            if "holding fee" in rent.lower() or "taken" in rent.lower() or "under" in rent.lower() or "leased" in rent.lower() or "approved" in rent.lower() or "negotiable" in rent.lower() or "offers welcome!" in rent.lower() or "contact agent" in rent.lower() or "price upon application" in rent.lower():
                return

            rent_week = rent.split("$")[-1].split("-")[0].replace(",","").lower().split("p")[0].split("/")[0].strip()
            rent_week = str(int(float(rent_week))*4)

            item_loader.add_value("rent", rent_week)
        item_loader.add_value("currency","AUD")

        address = " ".join(response.xpath("//h1[@class='listing-details__address']//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address",address)
        zipcode=response.xpath("//title//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(" ")[-1].strip()+" "+zipcode.split(" ")[-2].strip())
        city = " ".join(response.xpath("//h1[@class='listing-details__address']//text()[last()-1]").getall())
        if city:
            item_loader.add_value("city", city.strip())
        latitude="".join(response.xpath("//body[@class='listing-page']/comment()").extract())
        if latitude:
            item_loader.add_value("latitude",latitude.split("latitude")[-1].split(">")[0].replace("-","").replace('"',"").replace(":",""))
        longitude="".join(response.xpath("//body[@class='listing-page']/comment()").extract())
        if longitude:
            item_loader.add_value("longitude",longitude.split("longitude")[-1].split(">")[0].replace("-","").replace('"',"").replace(":",""))

        parking = response.xpath("//div[@class='listing-details__feature-group'][img[@alt='listing parking']]/span/text()").get()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking",False)
            else:
                item_loader.add_value("parking",True)

        desc = "".join(response.xpath("//div[@class='listing__description-container']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        

        images = [response.urljoin(x) for x in response.xpath("//div[@class='listing-carousel__container']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath("landlord_name", "//h3[@class='listing-agent-card__name']//text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='listing-agent-card__tel-numbers']/a[last()]/text()")
        phonecheck=item_loader.get_output_value("landlord_phone")
        if not phonecheck:
            item_loader.add_value("landlord_phone","+61 7 3229 3344")
        item_loader.add_value("landlord_email","brisbane@prd.com.au")

 
        yield item_loader.load_item()

        
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "flatshare" in p_type_string.lower():
        return "room"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "duplex" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    elif p_type_string and "bedroom" in p_type_string.lower():
        return "house"
    else:
        return None