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
    name = 'ausset_com'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = { 
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }
    def start_requests(self):
        yield Request("https://www.ausset.com/location/1", callback=self.parse)

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//article[@itemprop='itemListElement']//div[@class='titleArticle']"):
            seen = True
            follow_url = response.urljoin(item.xpath("./h2/a/@href").get())
            property_type = item.xpath("./h3/text()").get()
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
                
        if page == 2 or seen:
            yield Request(f"https://www.ausset.com/location/{page}", callback=self.parse, meta={"page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        if response.url == "https://www.ausset.com/":
            return
        item_loader.add_value("external_source", "Ausset_PySpider_france")   
        item_loader.add_xpath("title", "//h1[@class='titleBien']//text()")  
        room_count = response.xpath("//li[@class='data NB_CHAMBRES']//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[-1])
        else:
            room_count = response.xpath("//li[@class='data nbpieces']//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(":")[-1])
        address = response.xpath("//div[@id='quartier']//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("(")[0].replace("La ville de","").strip())
            item_loader.add_value("zipcode", address.split("(")[-1].split(")")[0].strip())
        external_id = response.xpath("//p[@class='ref']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())
        bathroom_count = response.xpath("//li[@class='data NB_SDB']//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[-1])
        else:
            bathroom_count = response.xpath("//li[@class='data NB_SE']//text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split(":")[-1])
        floor = response.xpath("//li[@class='data ETAGE']//text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(":")[-1].strip())
       
        square_meters = response.xpath("//li[@class='data surfaceHabitable']//text()").get()
        if square_meters:
            square_meters = square_meters.split(":")[-1].split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters.replace(",","."))))

        description = " ".join(response.xpath("//div[@class='offreContent']//p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        furnished = response.xpath("//h1[@class='titleBien']//text()[contains(.,'meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        elevator = response.xpath("//li[@class='data ASCENSEUR']//text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        terrace = response.xpath("//li[@class='data TERRASSE']//text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            elif "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
        balcony = response.xpath("//li[@class='data BALCON']//text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            elif "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
     
        parking = response.xpath("//li[@class='data GARAGE_BOX']//text() | //li[@class='data NB_PARK_EXT']//text() ").get()
        if parking:
            item_loader.add_value("parking", True)
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='slider_Mdl']/li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
 
        rent = "".join(response.xpath("//li[@class='data formatPrix']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ","").replace("\xa0",""))
        deposit = response.xpath("//li[@class='data formatteddepotgarantie']//text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[-1].split("€")[0].replace(" ","").replace("\xa0",""))
        utilities = response.xpath("//li[@class='data ChargesAnnonce']//text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[-1].split("€")[0].replace(" ","").replace("\xa0",""))
        script_map = response.xpath("//script[contains(.,'center: { lat :')]/text()").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("center: { lat :")[1].split(",")[0].strip())
            item_loader.add_value("longitude", script_map.split("center: { lat :")[1].split("lng:")[1].split("}")[0].strip())

        item_loader.add_value("landlord_name", "AUSSET IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 66 21 14 46")
        item_loader.add_value("landlord_email", "contact@ausset.com")
       
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