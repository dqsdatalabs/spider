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

class MySpider(Spider):
    name = 'elyseavenue_paris15_fr'
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    external_source="Elyseavenue_Paris15_PySpider_france"
    def start_requests(self):

        formdata = {
            'definition': '83bc957fe36fccaf88cab3a0298d5f3e',
            'adddata': '{"customsearch_mysearch_456":{"config":{"_field":"tracker_field_biensAC3SSTYPE","class":"form-control search-slt","_trackerId":"4","_firstlabel":"Type de bien"},"name":"select","value":""}}',
            'searchid': 'mysearch',
            'offset': '0',
            'maxRecords': '25',
            'store_query': '',
            'page': 'biens_locations',
            'recalllastsearch': '0',
        }

        yield FormRequest(
            url="https://elyseavenue-paris15.fr/tiki-search_customsearch-customsearch",
            formdata= formdata,
            callback=self.parse,
        )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='row mb-5']/div/div[@class='caption_cont']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        title = response.xpath("//div[@class='px-2']/h2[@class='text-capitalize']/text()").extract_first()
        if get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")

        item_loader.add_value("external_source", self.external_source)
        
        externalid= response.url.split("=")[1]
        if externalid:
            item_loader.add_value("external_id", externalid)
        dontallow=response.xpath("//h2[@class='text-capitalize']/text()").get()
        if dontallow and "parking" in dontallow.lower():
            return 

        

        energy_label =response.xpath("//div[@class='p-2 row']/button/span[1]/text()[not(contains(.,'Non communiqué'))]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        address =response.xpath("//div[@class='px-2']/h2[@class='text-grey text-capitalize']/text()").get()
        if address:
            item_loader.add_value("address", address)

        item_loader.add_value("city", "Paris")
        
        deposit ="".join(response.xpath("//div[@class='px-2 w-50 mx-auto text-left']/text()[contains(.,'garantie')]").getall())
        if deposit:
            dep = deposit.split(":")[1].replace(".","").split("€")[0]
            item_loader.add_value("deposit", dep.strip())

        utilities ="".join(response.xpath("//div[@class='px-2 w-50 mx-auto text-left']/text()[contains(.,'locataire ')]").getall())
        if utilities:
            uti = utilities.split(":")[1].replace(".","").split("€")[0]
            item_loader.add_value("deposit", uti.strip())

        rent =response.xpath("//div[@class='px-2']//h1[@class='prix font-weight-bold']/text()").get()
        if rent:
            rent = rent.replace(".","").replace("CC","").strip()
            item_loader.add_value("rent_string", rent)

        room_count ="".join(response.xpath("//div[@class='card-title text-center']/div/h2[2]/text()[contains(.,'pièce')]").getall())
        if room_count:
            room =room_count.split("pièce")[0]
            item_loader.add_value("room_count", room.strip())
        bathroom_count =response.xpath("//button[@class='btn btn-sm btn-secondary px-2 my-1 no_cursor text-primary rounded-pill'][4]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        squaremeters=" ".join(response.xpath("//div[@class='card-title text-center']/div/h2[2]/text()[contains(.,'m')]").getall())
        if squaremeters:
            squaremeters =squaremeters.split("•")[1].split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(squaremeters)))
        desc = "".join(response.xpath("//div[@class='p-2']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())          
            item_loader.add_value("description", desc)
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']/div[@class='carousel-item']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        # floor_plan_images = [x for x in response.xpath("//div[@class='modal-content']/img/@src").getall()]
        # if floor_plan_images:  
        #     image=str("".join(floor_plan_images))       
        #     if "estate" in image.lower():
        #         item_loader.add_value("floor_plan_images", floor_plan_images)
        # LatLng="".join(response.xpath("//script[contains(.,'lng')]/text()").getall())
        # if LatLng:
        #     latlngindex=LatLng.find("lat")
        #     latlng=LatLng[latlngindex:]
        #     latlng=latlng.split("}")[0]
        #     lat=latlng.split("lat:")[-1].split(",")[0]
        #     if lat:
        #         item_loader.add_value("latitude",lat)
        #     lng=latlng.split("lng:-")[-1]
        #     if lng:
        #         item_loader.add_value("longitude",lng)
        # features =response.xpath("//div[@class='row det-am']//div/span/text()").getall()
        # if features:
        #     for i in features:
        #         if "garage" in i.lower() or "parking" in i.lower():
        #             item_loader.add_value("parking", True)  
        #         if "terrace" in i.lower():
        #             item_loader.add_value("terrace", True) 
        #         if "balcony" in i.lower():
        #             item_loader.add_value("balcony", True)
        #         if "furnished" in i.lower():
        #             item_loader.add_value("furnished", True)
        item_loader.add_value("landlord_name", "ELYSE AVENUE")
        item_loader.add_value("landlord_phone", "+33 1 56 56 80 30")
        item_loader.add_value("landlord_email", "paris15@elyseavenue.com")


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower() or "appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None