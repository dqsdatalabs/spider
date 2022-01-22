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
import dateparser 

class MySpider(Spider):
    name = 'bonnivers_be' 
    execution_type='testing'
    country='belgium'
    locale='fr' 
    def start_requests(self):


        start_urls = [
            {
                "url" : [
                    "https://www.bonnivers.be/Rechercher/APPARTEMENTS%20Locations%20/Locations/Type-5%7CAPPARTEMENTS/Localisation-/Prix-/Tri-PRIX%20ASC,COMM%20ASC,CODE",
                ],
                "property_type" : "apartment"
            }, 
            {
                "url" : [
                    "https://www.bonnivers.be/Rechercher/MAISON%20Locations%20/Locations/Type-1%7CMAISON/Localisation-/Prix-/Tri-PRIX%20ASC,COMM%20ASC,CODE",
                ],
                "property_type" : "house"
            },   
            {
                "url" : [
                    "https://www.bonnivers.be/Rechercher/APPARTEMENTS%7CM%20Locations%20/Locations/Type-5%7CAPPARTEMENTS%7CM/Localisation-/Prix-/Tri-PRIX%20ASC,COMM%20ASC,CODE",
                ],
                "property_type" : "apartment"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING 
    def parse(self, response):
        # url = "https://www.bonnivers.be/fiche/Appartement%20Ã%C2%A0%20louer%20WAVRE/Code-00019258065/Lang-FR"
        # yield Request(url,callback=self.populate_item,meta={'property_type': "house"})

        for item in response.xpath("//a[contains(@class,'zoom-cont2 hvr-grow')]/@href").extract():
            follow_url = response.urljoin(item) 
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[.='»']/@href").get()
        if next_page and "bonnivers.be:443" not in next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Bonnivers_Be_PySpider_" + self.country + "_" + self.locale)
        prop_type = response.xpath("//h1[@class='liste-title']//text()[contains(.,'Commercial')]").get()
        if prop_type:
            return 
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//div[@class='col-md-6']/h2/text()")

        address = response.xpath("//div[@class='col-md-12']//iframe/@src").get()
        if address:
            item_loader.add_value('address', address.split("q=")[-1].split("+")[0])
        zipcode=response.xpath("//div[@class='col-md-12']//iframe/@src").get()
        if zipcode:
            zipcode=re.findall("\d+",zipcode)
            item_loader.add_value("zipcode",zipcode[0])
        city=response.xpath("//div[@class='col-md-12']//iframe/@src").get()
        if city:
            city=city.split("q=")[-1].split("+")[0]
            city=re.findall("\D",city)
            item_loader.add_value("city", "".join(city))

        item_loader.add_xpath("external_id", "//div[@class='ref-tag']/a/span/b/text()")

        meters = "".join(response.xpath("substring-after(//div[@class='col-xs-4']/text()[contains(.,'Superficie')],': ')").extract())
        if meters:
            item_loader.add_value("square_meters", meters.replace("m²","")) 

        room = "".join(response.xpath("substring-before(//div[@class='col-xs-4']/text()[contains(.,'chambre')],'chambre')").extract())
        if room:
            item_loader.add_value("room_count", room.strip())
        elif not room:
            room = "".join(response.xpath("//tr[td[.='Chambres']]/td[2]/text()").extract())
            if room:
                item_loader.add_value("room_count", room.strip())
        else:
            room1="".join(response.xpath("substring-before(//div[@class='col-xs-4']/text()[contains(.,'chambre(s)')],'chambre(s)')").extract())
            if room1:
                item_loader.add_value("room_count", room1.strip())


        bathroom = "".join(response.xpath("substring-before(//div[@class='col-xs-4']/text()[contains(.,'salle')],'salle')").extract())
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())

        price = "".join(response.xpath("//tr[td[.='Prix']]/td[2]/text()").extract())
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))

        utilities = "".join(response.xpath("//tr[td[.='Charges']]/td[2]/text()").extract())
        if utilities:
            item_loader.add_value("utilities", utilities.replace(" ",""))

        e_label = ""
        label = "".join(response.xpath("substring-before(substring-after(//div[contains(@class,'container')]/div/div/img/@src,'/FR/'),'.')").extract())
        if label:
            if "p" in label:
                e_label = label.replace("p","+")
            elif "pp" in label:
                e_label = label.replace("p","++")
            else:
                e_label = label
            item_loader.add_value("energy_label", e_label.upper())

        available_date=response.xpath("//tr[td[.='Disponibilité']]/td[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip())
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        desc = " ".join(response.xpath("//div[@class='col-md-6']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc)

        images=[x for x in response.xpath("//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))


        terrace = "".join(response.xpath("//tr[td[.='Terrasse']]/td[2]/text()").extract())
        if terrace:
            if "Non" in terrace:
                item_loader.add_value("terrace", False)
            else:
                if terrace or "Oui" in terrace:
                    item_loader.add_value("terrace", True)

        furnished = "".join(response.xpath("//tr[td[.='Meublé']]/td[2]/text()").extract())
        if furnished:
            if "Non" in furnished:
                item_loader.add_value("furnished", False)
            else:
                if terrace or "Oui" in terrace:
                    item_loader.add_value("furnished", True)

        parking = " ".join(response.xpath("//tr[td[.='Equipement extérieur']]/td[2]/text()").extract())
        if parking:
            if "parking" in parking or "Garage" in parking :
                item_loader.add_value("parking", True)
            else:
                park = " ".join(response.xpath("//tr[td[.='Equipement intérieur']]/td[2]/text()[contains(.,'parking') or contains(.,'Garage')]").extract())
                if park:
                    item_loader.add_value("parking", True)
                else:
                    park = response.xpath("//tr[td[.='Emplacements parking']]/td[2]/text()").extract_first()
                    if park:
                        item_loader.add_value("parking", True)

        balcony = "".join(response.xpath("//tr[td[.='Equipement extérieur']]/td[2]/text()[contains(.,'Balcon')]").extract())
        if balcony:
            item_loader.add_value("balcony", True)

        item_loader.add_value("landlord_email", "jodoigne@bonnivers.be")
        item_loader.add_value("landlord_name", "Bonnivers")
        item_loader.add_value("landlord_phone", "010.81.43.03")

        if not item_loader.get_collected_values("parking"):
            parking = response.xpath("//td[contains(.,'Emplacements parking')]/following-sibling::td/text()").get()
            parking = int("".join(filter(str.isnumeric, parking))) if parking else parking
            if parking:
                if parking > 0: item_loader.add_value("parking", True)
                else: item_loader.add_value("parking", False)

        if not item_loader.get_collected_values("title"): item_loader.add_xpath("title", "//title/text()")

        map_url = response.xpath("//iframe[contains(@src,'maps/embed')]/@src").get()
        if map_url: yield Request(map_url, callback=self.get_latlng, meta={"item_loader": item_loader})
        else: yield item_loader.load_item()
    
    def get_latlng(self, response):

        item_loader = response.meta["item_loader"]

        try:
            latlong = response.xpath("//div[@id='mapDiv']/following-sibling::script/text()").get()
            if latlong:
                item_loader.add_value("latitude", latlong.split('",null,[null,null,')[1].split(',')[0].strip())
                item_loader.add_value("longitude", latlong.split('",null,[null,null,')[1].split(',')[1].split(']')[0].strip())
        except:
            pass
        yield item_loader.load_item()