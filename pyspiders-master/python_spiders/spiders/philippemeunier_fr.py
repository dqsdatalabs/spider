# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from python_spiders.loaders import ListingLoader
import re
class MySpider(Spider):
    name = 'philippemeunier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    # 1. FOLLOWING
    def start_requests(self): 
        formdata = {
            "data[Search][offredem]": "2",
            "data[Search][idtype]": "2",
            "data[Search][surfmin]": "",
            "data[Search][surfmax]": "",
            "data[Search][pieces]": "void",
            "data[Search][idvillecode]": "void",
            "data[Search][NO_DOSSIER]": "",
            "data[Search][distance_idvillecode]": "",
            "data[Search][prixmin]": "0",
            "data[Search][prixmax]": "3847",
        }
        yield FormRequest(
            "https://www.philippemeunier.fr/recherche/",
            callback=self.parse,
            formdata=formdata
            )
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for url in response.xpath("//div[@class='backBtn']//a[span[@class='pull-left']]/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.philippemeunier.fr/recherche/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall()) 
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        elif response.xpath("//h2/text()").getall() and get_p_type_string("".join(response.xpath("//h2/text()").getall())):
            item_loader.add_value("property_type", get_p_type_string("".join(response.xpath("//h2/text()").getall())))
        else:
            return
        
        
        item_loader.add_value("external_source", "Philippemeunier_PySpider_france")
        title = " ".join(response.xpath("//div[@class='bienTitle']//h2/text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ',title))
            item_loader.add_value("address", title.split(" - ")[-1])
        external_id = response.xpath("//span[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref")[-1].strip())
        zipcode = response.xpath("//p[span[contains(.,'Code postal')]]/span[2]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        city = response.xpath("//p[span[contains(.,'Ville')]]/span[2]/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
           
        floor = response.xpath("//p[span[contains(.,'Etage')]]/span[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        room_count = response.xpath("//p[span[contains(.,'chambre')]]/span[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//p[span[contains(.,'pièce')]]/span[2]/text()")

        item_loader.add_xpath("bathroom_count", "//p[span[contains(.,'salle')]]/span[2]/text()")
        square_meters = response.xpath("//p[span[contains(.,'Surface habitable')]]/span[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split(" m")[0].strip().replace(",","."))))
      
        if description:
            item_loader.add_value("description", description.strip())
     
        furnished = response.xpath("//p[span[contains(.,'Meublé')]]/span[2]/text()[not(contains(.,'Non renseigné'))]").get()
        if furnished:
            if furnished.lower().strip() =="non":
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        elevator = response.xpath("//p[span[contains(.,'Ascenseur')]]/span[2]/text()").get()
        if elevator:
            if elevator.lower().strip() =="non":
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        parking = response.xpath("//p[span[contains(.,' parking') or contains(.,' garage')]]/span[2]/text()").get()
        if parking:
            if parking.lower() =="0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]/li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//script[contains(.,'center: { lat :')]/text()").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("center: { lat :")[1].split(",")[0].strip())
            item_loader.add_value("longitude", script_map.split("center: { lat :")[1].split("lng:")[1].split("}")[0].strip())
        rent = response.xpath("//p[span[contains(.,'Loyer CC* / mois')]]/span[2]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        deposit = response.xpath("//p[span[contains(.,'Dépôt de garantie ')]]/span[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ","").strip())
        utilities = response.xpath("//p[span[contains(.,'Charges')]]/span[2]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(" ","").split("€")[0].strip())
        item_loader.add_value("landlord_name", "AGENCE DE L'ANJOU")
        item_loader.add_value("landlord_phone", "02 41 87 52 52")
        item_loader.add_value("landlord_email", "location@agencedelanjou.com")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower() or "residential" in p_type_string.lower() or "conversion" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "maison" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None