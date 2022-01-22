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
    name = 'immodefrance_rhone_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {
        "PROXY_ON":True,
    }

    def start_requests(self):

        start_urls = [
            {
                "type" : 1,
                "property_type" : "apartment"
            },
            
        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))
            payload = {
                "location_search[commune]": "",
                "location_search[typeBien][]": r_type,
                "location_search[loyer_min]": "0",
                "location_search[loyer_max]": "1000000",
                "location_search[surface_min]": "",
                "location_search[noMandat]": "",
                "location_search[tri]": "loyerCcTtcMensuel|asc",
            }

            yield FormRequest(url="https://www.immodefrance-rhone.fr/fr/locations",
                                callback=self.parse,
                                formdata=payload,
                                #headers=self.headers,
                                meta={'property_type': url.get('property_type')})
        
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 0)

        seen = False
        for item in response.xpath("//div[@class='article ']/a/@href").extract():
            yield Request(
                url=item,
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")}, 
            )
            seen = True
        
        if page == 0 or seen:
            p_url = f"https://www.immodefrance-rhone.fr/fr/map/mini-fiche/Location/{page}/normal/loyerCcTtcMensuel%7Casc"
            headers = {
                ":path": "/fr/map/mini-fiche/Location/20/normal/loyerCcTtcMensuel%7Casc",
                "referer": "https://www.immodefrance-rhone.fr/fr/locations",
                "accept-encoding": "gzip, deflate, br"
            }
            yield Request(
                url=p_url,
                callback=self.parse,
                headers=headers,
                meta={"property_type" : response.meta.get("property_type"), "page":page+20}, 
            )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Immodefrancerhone_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("(//h1/text())[1]").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        rent="".join(response.xpath("//div[@class='prix_resp']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))

        
        square_meters=response.xpath("//div[@class='criteres']/div[contains(.,'habitable')]/text()").get()
        if "." in square_meters:
            square_mt=square_meters.split(":")[1].split("m²")[0].strip()
            item_loader.add_value("square_meters", str(round(float(square_mt))))
        elif square_meters:
            item_loader.add_value("square_meters", square_meters.split(":")[1].split("m²")[0].strip())
        bathroom_count=response.xpath("//div[@class='criteres']/div[contains(.,'salle de bain') or contains(.,'Salle d') ]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1])
        room_count=response.xpath("//div/h2[@class='clr-location']//text()[contains(.,'pièce')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" pièce")[0].split(" ")[-1])
        else:
            room_count=response.xpath("//p[@class='chambre']/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(" ")[0].strip())
        
        attr="".join(response.xpath("//div[@class='titre']/h3/text()").getall())
        if attr:
            address=attr.split("|")[0].replace('(','').replace(')','')
            item_loader.add_value("address", address.strip())
            try:
                city = attr.split("|")[0].split("(")[0].strip()
                zipcode = attr.split("|")[0].split("(")[1].split(")")[0].strip()
                if "Lyon" in city:
                    city = city.split(" ")[0].strip()
                item_loader.add_value("city", city.strip())
                item_loader.add_value("zipcode", zipcode.strip())
            except:
                pass
            try:
                ref=attr.split('|')[1].split(':')[1]
                item_loader.add_value("external_id", ref.strip())
            except:
                pass

        desc="".join(response.xpath("//div[@class='descriptif']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            if "meublé" in desc:
                item_loader.add_value("furnished", True)
        images=[x for x in response.xpath("//div[@id='carousel-photo']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_phone","04 72 75 40 40")
        item_loader.add_value("landlord_name","IMMO de France Rhone")
        item_loader.add_value("landlord_email","rhonealpes@immodefrance.com")
        
        floor=response.xpath("//div[@class='criteres']/div[contains(.,'Étage')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(":")[1].strip())
            
        utilities=response.xpath("//div[@class='criteres']/div[contains(.,'dont état des lieux')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].strip().replace(" ",""))
        
        deposit=response.xpath("//div[@class='criteres']/div[contains(.,'garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0].strip().replace(" ",""))
        
        energy_label=response.xpath("//table[contains(@class,'tableau_conso')]/@class").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("dpe_")[1])
            
        parking=response.xpath("//p[@class='parking']/text()").get()
        garage=response.xpath("//div[@class='criteres']/div[contains(.,'garage')]/text()").get()
        if parking or garage:
            item_loader.add_value("parking",True)

        latitude_longitude = response.xpath("//script[contains(.,'position = [')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('position = [')[1].split(',')[0]
            longitude = latitude_longitude.split('position = [')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        yield item_loader.load_item()