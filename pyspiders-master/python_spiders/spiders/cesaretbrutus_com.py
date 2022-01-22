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
    name = 'cesaretbrutus_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "https://www.cesaretbrutus.com/recherche/"
    current_index = 0
    other_prop = ["1"]
    other_type = ["house"]
    external_source="Cesaretbrutus_PySpider_france"
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
        formdata = {
            "data[Search][offredem]": "2",
            "data[Search][idtype][]": "2",
            "data[Search][pieces]": "void",
            "data[Search][prix]": "void",
            "data[Search][NO_DOSSIER]": "",
            "data[Search][distance_idvillecode]": "",
            "data[Search][prixmin]": "",
            "data[Search][surfmin]": "",
        }
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment"})

            
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for url in response.xpath("//div[contains(@class,'card col')]/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 2 or seen:            
            p_url = f"https://www.cesaretbrutus.com/recherche/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": self.other_prop[self.current_index],
                "data[Search][pieces]": "void",
                "data[Search][prix]": "void",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][distance_idvillecode]": "",
                "data[Search][prixmin]": "",
                "data[Search][surfmin]": "",
            }
            yield FormRequest(self.post_url,
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': self.other_type[self.current_index],})
            self.current_index += 1

                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1/span/text()")
        external_id = response.xpath("//h2[@itemprop='potentialAction']/span[@class='labelprix ref']/b/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref")[-1].strip())
        zipcode = response.xpath("//tr[th[.='Code postal']]/th[2]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        city = response.xpath("//tr[th[.='Ville']]/th[2]/text()").get()
        if city:
            item_loader.add_value("city", city)
            if zipcode:
                city = city+", "+zipcode
            item_loader.add_value("address", city.strip())
        item_loader.add_xpath("floor", "//tr[th[.='Etage']]/th[2]/text()")
        item_loader.add_xpath("bathroom_count", "//tr[th[contains(.,'Nb de salle d')]]/th[2]/text()")
        
        room_count = response.xpath("//tr[th[.='Nombre de chambre(s)']]/th[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//tr[th[.='Nombre de pièces']]/th[2]/text()")
        
        description = " ".join(response.xpath("//p[@class='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        square_meters = response.xpath("//tr[th[contains(.,'Surface habitable')]]/th[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].split(",")[0])
        rent = response.xpath("//tr[th[.='Loyer CC* / mois']]/th[2]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        deposit = response.xpath("//tr[th[.='Dépôt de garantie TTC']]/th[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))
        utilities = response.xpath("//tr[th[contains(.,'Charges locatives')]]/th[2]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(" ",""))
        lat_lng = response.xpath("//script[contains(.,'center: { lat :')]/text()").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split("center: { lat :")[1].split(",")[0].strip())
            item_loader.add_value("longitude", lat_lng.split("center: { lat :")[1].split("lng:")[1].split("}")[0].strip())
       
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='imageGallery notLoaded']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)  
        item_loader.add_xpath("landlord_name", "//div[@class='media-body']/span[1]/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='media-body']/span[2]/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='media-body']//a[contains(@href,'mail')]/text()")

        furnished = response.xpath("//tr[th[.='Meublé']]/th[2]/text()").get()
        if furnished:
            if "NON" in furnished.upper():
                item_loader.add_value("furnished", False)
            elif "OUI" in furnished.upper():
                item_loader.add_value("furnished", True)
        elevator = response.xpath("//tr[th[.='Ascenseur']]/th[2]/text()").get()
        if elevator:
            if "NON" in elevator.upper():
                item_loader.add_value("elevator", False)
            elif "OUI" in elevator.upper():
                item_loader.add_value("elevator", True)
        balcony = response.xpath("//tr[th[.='Balcon']]/th[2]/text()").get()
        if balcony:
            if "NON" in balcony.upper():
                item_loader.add_value("balcony", False)
            elif "OUI" in balcony.upper():
                item_loader.add_value("balcony", True)
        terrace = response.xpath("//tr[th[.='Terrasse']]/th[2]/text()").get()
        if terrace:
            if "NON" in terrace.upper():
                item_loader.add_value("terrace", False)
            elif "OUI" in terrace.upper():
                item_loader.add_value("terrace", True)        
        yield item_loader.load_item()