# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
class MySpider(Spider):
    name = 'auditia_gestion_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        yield Request("http://www.auditia-gestion.com/fr/immobilier/louer/page-1", callback=self.parse)

    def parse(self, response):

        page = response.meta.get("page", 2)
        max_page = response.xpath("//div[@id='AnnoncesPagination']/a[last()]/text()").get()

        for item in response.xpath("//ul[contains(@class,'annonceList')]/li[not(@id)]"):
            follow_url = response.urljoin(item.xpath(".//a[@class='enSavoirPlus']/@href").get())
            property_type = item.xpath(".//h3/text()").get()
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
                
        if page <= int(max_page):
            yield Request(f"http://www.auditia-gestion.com/fr/immobilier/louer/page-{page}", callback=self.parse, meta={"page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "auditia_gestion_PySpider_france_fr")      
        item_loader.add_xpath("title", "//div[@id='PresentationAnnonce']/h4/text()")
        item_loader.add_xpath("external_id", "substring-after(//div[@id='PresentationAnnonce']/p[@class='refAnnonce']/text(),'ref.')")
        
        room_count = response.xpath("//li[strong[.='Nbre de chambres']]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count","//li[strong[.='Nbre de pièces']]/text()")
        bathroom_count = response.xpath("//p[@class='descriptif']//text()[contains(.,'salle de bain')]").get()
        if bathroom_count:
            bathroom_value = ""
            bathroom = bathroom_count.split("salle de bain")[0].strip().split(" ")[0].strip()
            if bathroom.isdigit():
                bathroom_value = bathroom
            elif bathroom.lower() == "une" or bathroom.lower() == "un":
                bathroom_value = "1"
            elif bathroom.lower() == "deux":
                bathroom_value = "2"
            if bathroom_value:
                item_loader.add_value("bathroom_count", bathroom_value)
        address = response.xpath("//div[@id='PresentationAnnonce']/h4/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[-1].split(")")[0].strip())
        energy_label = response.xpath("//div[@id='GraphConsommation']//span[@class='graphArrowValue']//text()[.!='NC']").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))        
        
        square_meters = response.xpath("//li[strong[.='Surface habitable']]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split("m")[0].strip())))
      
        description = " ".join(response.xpath("//p[@class='descriptif']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
            
        available_date = response.xpath("//p[@class='descriptif']//text()[contains(.,'DISPONIBLE A PARTIR DU')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("PARTIR DU")[-1].strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        else:
            available_date = response.xpath("//p[@class='descriptif']//text()[contains(.,'DISPONIBLE LE')]").get()
            if available_date:
                date_parsed = dateparser.parse(available_date.split("DISPONIBLE LE")[1].strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        images = [x for x in response.xpath("//div[@id='BandeauPhotos']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
 
        rent = response.xpath("//p[@class='prix']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.split(":")[-1].replace(" ",""))
        utilities = response.xpath("//p[@class='descriptif']//text()[contains(.,'CHARGES :') and not(contains(.,'LOYER '))]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[-1].split("€")[0])
        else: 
            utilities = response.xpath("//p[@class='descriptif']//text()[contains(.,'de provision')]").get()
            if utilities:
                utilities = utilities.split("de provisions")[0]
                if "+" in utilities:
                    utilities = utilities.split("+")[1].replace("CHARGES:","").strip()
                    if "euro" in utilities.lower():
                        utilities = utilities.split(" ")[0]
                    else:
                        utilities = utilities.split(" ")[-1]
                    utilities = utilities.split(",")[0]
                else:
                    utilities = utilities.strip().split(" ")[-2]
                if utilities.isdigit():
                    item_loader.add_value("utilities", int(float(utilities)))
        deposit = response.xpath("//p[@class='descriptif']//text()[contains(.,'DEPOT DE GARANTIE :')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[-1].split(".")[0].split(",")[0])
        item_loader.add_value("landlord_name", "Auditia - Gestion immobiliere")
        item_loader.add_value("landlord_phone", "05 62 30 66 20")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label