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
    name = 'immodome_com'   
    execution_type = 'testing'
    country = 'france'
    locale = 'fr' 
    def start_requests(self, **kwargs):

        if not kwargs:
            kwargs = {"cat":"5"}

        for key, value in kwargs.items():
            formdata = {
                "cat": value,
                "commune": "Toutes",
                "mandat": "",
            }
            yield FormRequest("https://www.immodome.com/W2serveur.php?fnc=rechercheBien",
                            callback=self.parse,
                            formdata=formdata)


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='bienliste']/figure"):
            p_type = item.xpath(".//figcaption//span[contains(.,'m²')]/text()").get()
            if get_p_type_string(p_type):
                prop_type = get_p_type_string(p_type)
            else:
                prop_type = None
            p_data = item.xpath(".//img/@onclick").get().split("OuvreVue(")[1].split(")")[0].replace("'", "")
            formdata = {
                "vue": p_data.split(",")[0].strip(),
                "id": p_data.split(",")[1].strip()
            }
            yield FormRequest(
                "http://immodome.com/W2serveur.php?fnc=getVue",
                callback=self.populate_item,
                formdata=formdata,
                meta={
                    "prop_type" : prop_type,
                }
            )
          
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", "http://immodome.com/")
        
        prop_type = response.meta["prop_type"]
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            property_type = " ".join(response.xpath("//div[@id='bienDescriptif']/span[@id='vpdescr']//text()").getall()).strip()   
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))

        item_loader.add_value("external_source","Immodome_PySpider_france")

        title = response.xpath("//span[@id='vpnom']/text()").get()
        if title:
            item_loader.add_value("title", title)
        if "meublé" in title:
            item_loader.add_value("furnished", True)

        utilities = response.xpath("//span[@id='vpdescr']//text()[contains(.,'provision pour charge')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())

        deposit = "".join(response.xpath("substring-after(//span[@id='vpdescr']/p/span[contains(.,'Dépôt de garantie')],'Dépôt de garantie : ')").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip())

        description = " ".join(response.xpath("//div[@id='bienDescriptif']/span[@id='vpdescr']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        rent = "".join(response.xpath("//div[@id='bienDescriptif']/span[@id='vpprix']//text()").getall())
        if rent:
            price = rent.replace(" ","")
            item_loader.add_value("rent_string", price)

        energy_label = "".join(response.xpath("substring-after(//div[@id='bienDescriptif']/span[@id='vpdescr']/p[contains(.,'ENERGIE ')],': ')").getall())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(".")[0].strip())

        address = "".join(response.xpath("//div[@class='biendescriptif']/span[@id='vpcommune']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())

        images = [ response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        room_count = "".join(response.xpath("//div[@id='bienDescriptif']/span[@id='vpnom']//text()[contains(.,'pièces')]").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.split("pièces")[0].strip().split(" ")[1].strip())
        else:
            item_loader.add_value("room_count","1")

        square_meters = " ".join(response.xpath("//div[@id='bienDescriptif']/span[@id='vpnom']//text()[contains(.,'m²')]").getall()).strip()   
        if square_meters:
            
            meters =  square_meters.split("m²")[0].split(",")[1].strip()
            if "pièces" in meters:
                meters = square_meters.split("m²")[0].strip().split(",")[-1]
            item_loader.add_value("square_meters",meters)

        item_loader.add_value("landlord_phone", "06.61.57.82.50")
        item_loader.add_value("landlord_name", "Agence Immobilière  IMMO DÔME")
        item_loader.add_value("landlord_email", "agence@immodome.com")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "maison" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None

