# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser

class MySpider(Spider):
    name = 'agencecruz_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    headers = {
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            "origin": "https://www.agencecruz.com"
        }
    def start_requests(self):

        data = {
          "page": "1",
          "orderBy": "lot_random",
        }
       
        url = "https://www.agencecruz.com/ajax.req.4g.php?id=getListe"
        yield FormRequest(
            url,
            formdata=data,
            headers=self.headers,
            callback=self.parse,
            meta={"page": 1}
        )
    
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page")
        
        seen = False
        for item in response.xpath("//div[contains(@class,'listTab')]"):
            follow_id = item.xpath(".//div[@class='green_btn']/a/@href").extract_first().split(",")[1].strip("'")
            price = item.xpath("substring-before(.//h3[contains(.,'€')]/text(), ',')").extract_first()
            city = item.xpath("normalize-space(./div[3]/text()[2])").extract_first()
            external_id = item.xpath(".//div[contains(@class,'liste_col')]//h3[not(contains(.,'€'))]/text()").extract_first()
            external_id = external_id.strip().replace('"',"").replace(" ","-").lower().replace("'","-")
            data = {
                "lot_no": f"{follow_id}",
                "page": f"{str(page)}",
                "orderBy": "lot_random",
            }
            
            url = "https://www.agencecruz.com/ajax.req.4g.php?id=getFiche"
            yield FormRequest(
                url,
                formdata=data,
                headers=self.headers,
                callback=self.populate_item,
                meta={"price":price, "city": city, "external_id": external_id}
            )
            seen = True
            
        if seen:
            page += 1
            data = {
              "page": f"{str(page)}",
              "orderBy": "lot_random",
            }
           
            url = "https://www.agencecruz.com/ajax.req.4g.php?id=getListe"
            yield FormRequest(
                url,
                formdata=data,
                headers=self.headers,
                callback=self.parse,
                meta={"page": page}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_xpath("title", "//div[@class='Fiche_Infos']/h4[1]/text()")

        item_loader.add_value("external_link", "https://www.agencecruz.com/reservation/resultats/")
        item_loader.add_value("external_id", response.meta.get('external_id'))
        
        item_loader.add_value("external_source", "Agencecruz_PySpider_"+ self.country + "_" + self.locale)

        latitude_longitude = response.xpath("//a[.='Google MAP']/@href").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(',')[-2].strip('\'')
            longitude = latitude_longitude.split(',')[-1].strip(');').strip('\'')
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        title = response.xpath("//div[@class='Fiche_Infos']/h4[1]/text()").get()
        property_type = ''
        if title:
            if 'studio' in title.lower():
                property_type = 'studio'
                item_loader.add_value("property_type", property_type)
            elif 'cabine' in title.lower() or 'chalet' in title.lower():
                property_type = 'house'
                item_loader.add_value("property_type", property_type)

        description = response.xpath("//div[@class='Fiche_Detail mb10']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip().strip('\r\n') + ' '
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        
        if desc_html:
            square_meters = desc_html.split('m²')[0].strip().split(' ')[-1].strip()
            allow = True
            try:
                int(square_meters)
            except:
                allow = False
            if allow:
                item_loader.add_value("square_meters", square_meters)
        
        if desc_html:
            
            room_count = desc_html.split('pièces')[0].strip().split(' ')[-1].strip()
            room = response.xpath("//div[@class='Fiche_Infos']/h4[1]/text()").extract_first()
            if room == "Studio":
                item_loader.add_value("room_count", "1")
            else:
                allow = True
                try:
                    int(room_count)
                except:
                    allow = False
                if allow:
                    item_loader.add_value("room_count", room_count)
                if room:
                    item_loader.add_value("room_count", room_count.split("pièces")[0].strip())
                else:
                    item_loader.add_xpath("room_count", "substring-before(//div[@class='row Fiche']/div/div/h4[contains(.,'pièces')]/text(), ' ')")
            

        item_loader.add_value("currency", 'EUR')

        rent = response.meta.get("price")
        if rent:
            allow = True
            try:
                float(rent)
            except:
                allow = False
            if allow:
                item_loader.add_value("rent", rent)

        external_id = response.xpath("//h4[contains(.,'Réf.')]/text()").get()
        if external_id:
            external_id = external_id.strip('Réf. ').strip()
            item_loader.add_value("external_id", external_id)

        city = response.meta.get("city")
        if city:
            item_loader.add_value("city", city)
            item_loader.add_value("address", city)

        images1 = [x for x in response.xpath("//div[@class='Fiche_PhotoPrincipale']//img/@src").getall()]
        images2 = [x for x in response.xpath("//div[@class='Fiche_sliderContainer']//img/@src").getall()]
        images = images1 + images2
        if images:
            item_loader.add_value("images", list(set(images)))
            item_loader.add_value("external_images_count", str(len(images)))

        if desc_html:
            pets_allowed = None
            if 'animaux non' in desc_html.lower():
                pets_allowed = False
            elif 'animaux' in desc_html.lower():
                pets_allowed = True
            if pets_allowed:
                item_loader.add_value("pets_allowed", pets_allowed)

        if desc_html:
            floor = desc_html.split('ème étage')[0].strip().split(' ')[-1].strip()
            allow = True
            try:
                int(floor)
            except:
                allow = False
            if allow:
                item_loader.add_value("floor", floor)
        
        features = response.xpath("//div[@class='Fiche_Infos']//text()").getall()
        desc_html2 = ''      
        if features:
            for f in features:
                desc_html2 += f.strip().strip('\r\n') + ' '

        if desc_html or desc_html2:
            if 'parking' in desc_html.lower() or 'parking' in desc_html2.lower():
                parking = True
                item_loader.add_value("parking", parking)

        if desc_html or desc_html2:
            if 'ascenseur' in desc_html.lower() or 'ascenseur' in desc_html2.lower():
                elevator = True
                item_loader.add_value("elevator", elevator)

        if desc_html or desc_html2:
            if 'balcon' in desc_html.lower() or 'balcon' in desc_html2.lower():
                balcony = True
                item_loader.add_value("balcony", balcony)

        if desc_html or desc_html2:
            if 'terrasse' in desc_html.lower() or 'terrasse' in desc_html2.lower():
                terrace = True
                item_loader.add_value("terrace", terrace)

        if desc_html or desc_html2:
            if 'lave linge' in desc_html.lower() or 'lave linge' in desc_html2.lower():
                washing_machine = True
                item_loader.add_value("washing_machine", washing_machine)

        if desc_html or desc_html2:
            if 'lave-vaisselle' in desc_html.lower() or 'lave-vaisselle' in desc_html2.lower():
                dishwasher = True
                item_loader.add_value("dishwasher", dishwasher)
        
        item_loader.add_value("landlord_name", "AGENCE CRUZ")
        item_loader.add_value("landlord_phone", "+33 (0)4 50 73 24 94")
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data