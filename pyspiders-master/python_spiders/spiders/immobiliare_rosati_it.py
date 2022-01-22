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
    name = 'immobiliare_rosati_it'
    external_source = "ImmobiliareRosati_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it'
    url = "https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=B9596B80-0091-47BB-A62F-BC08D4D01288&querystring=%3FfromSearch%3Dtrue%26Contratto%3DA%26Categoria%3D1%26Provincia%3D%26Comune%3D&clientId=&clientClassName=&numeroColonne=2&lingua=it&numeroRisultati=4&urlPaginaRisultati=&urlPaginaAnnuncio=&urlPaginaCantiere=undefined&mostraTitoli=true&mostraZonaQuartiere=true&mostraDescrizione=true&mostraGallery=true&carousel=false&mostraDettagli=true&mostraBottoni=true&mostraAnnunciSalvati=true&mostraIcone=true&mostraPrivacy=true&urlPrivacy=%2Fit%2Fprivacy&startRisultati=-1&RewriteProvincia=&RewriteComune=&RewriteQuartiere=&RewriteTipologia=&tolerancePrezzo=0&toleranceSuperficie=0&toleranceLocali=false&annunciSalvati=&_=1632375288949"
    
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'Origin': 'https://www.immobiliare-rosati.it/',
        'Accept-Language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4'
    }
    
    def start_requests(self):
        yield Request(
            url=self.url,
            callback=self.parse,
            headers=self.headers
        )
        
    # 1. FOLLOWING
    def parse(self, response):
        data = ""
        try:
            data = json.loads(response.body)
            data = json.loads(data)["html"]
        except:
            data = response.body.split(b">")[1].split(b"<")[0].strip()
            data = json.loads(data)["html"].replace("&lt;","<").replace("&gt;",">")

        sel = Selector(text=data, type="html")  

        page = response.meta.get('page', 4)
        
        seen = False
        for item in sel.xpath("//div[@class='gx-risultato']"):
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", "https://www.immobiliare-rosati.it/?fromSearch=true&Contratto=A&Categoria=1&Provincia=&Comune=#gx-annunci-lista")
            item_loader.add_value("external_source", self.external_source)

            external_id = item.xpath("./a/@href").get()
            if external_id:
                external_id = external_id.split("(")[1].split(",")[0]
                item_loader.add_value("external_id", external_id)

            prop_type = item.xpath(".//h2/text()").get()
            if get_p_type_string(prop_type):
                item_loader.add_value("property_type", get_p_type_string(prop_type))
            else: return
            
            title = item.xpath(".//h2/text()").get()
            item_loader.add_value("title", title)

            address = item.xpath(".//div[@class='gx-risultato-testo']/h3/text()").get()
            if address:
                item_loader.add_value("address", address)
            item_loader.add_value("city", "Rome")

            rent = item.xpath(".//span[contains(.,'Prezzo')]/following-sibling::span/text()").get()
            if rent:
                item_loader.add_value("rent", rent.split("€")[1].strip())
            item_loader.add_value("currency", "EUR")

            room_count = item.xpath(".//span[contains(.,'Local')]/following-sibling::span/text()").get()
            item_loader.add_value("room_count", room_count)

                
            square_meters = item.xpath(".//span[contains(.,'Superficie')]/following-sibling::span/text()").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
            
            desc = "".join(item.xpath(".//div[@class='gx-risultato-description']//text()").getall())
            if desc:
                item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
            
            item_loader.add_value("landlord_name", "ROSATI IMMOBILIARE")
            item_loader.add_value("landlord_phone", "0649779184")
            item_loader.add_value("landlord_email", "rosatiimmobiliare@gmail.com")

            url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/dettaglio?apikey=B9596B80-0091-47BB-A62F-BC08D4D01288&IDImmobile={external_id}&runtimeId=4&pathName=%2F&clientId=&clientClassName=&lingua=it&urlPaginaAnnuncio=&urlPaginaCantiere=undefined&mostraTitolo=true&mostraBottoneStampa=true&mostraBottoniSocial=true&mostraBottoneInviaAmico=true&mostraDescrizione=true&mostraBottoneLeggiTutto=false&mostraBottoneCantiere=true&mostraMappa=true&mostraDettagli=true&mostraBottoneDettagli=true&mostraGallery=true&carousel=false&thumbnails=bottom&mostraVideo=true&mostraPlanimetrie=true&mostraVirtualTour=true&mostraRichiestaInfo=true&mostraPrivacy=true&urlPrivacy=%2Fit%2Fprivacy&richiestaCampoCustom1=&richiestaCampoCustom2=&richiestaCampoCustom3=&_=1632380981013"
            yield Request(url, callback=self.get_images, headers=self.headers, meta={"item_loader": item_loader})
            seen = True

        if page == 4 or seen:
            url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=B9596B80-0091-47BB-A62F-BC08D4D01288&querystring=%3FfromSearch%3Dtrue%26Contratto%3DA%26Categoria%3D1%26Provincia%3D%26Comune%3D&clientId=&clientClassName=&numeroColonne=2&lingua=it&numeroRisultati=4&urlPaginaRisultati=&urlPaginaAnnuncio=&urlPaginaCantiere=undefined&mostraTitoli=true&mostraZonaQuartiere=true&mostraDescrizione=true&mostraGallery=true&carousel=false&mostraDettagli=true&mostraBottoni=true&mostraAnnunciSalvati=true&mostraIcone=true&mostraPrivacy=true&urlPrivacy=%2Fit%2Fprivacy&sortBy=&startRisultati={page}&RewriteProvincia=&RewriteComune=&RewriteQuartiere=&RewriteTipologia=&tolerancePrezzo=0&toleranceSuperficie=0&toleranceLocali=false&annunciSalvati=&_=1632380981012"
            yield Request(url, callback=self.parse, meta={"page": page+4, "property_type": response.meta.get('property_type')})

    def get_images(self, response):
        data = json.loads(response.body)
        sel = Selector(text=json.loads(data)["html"], type="html")
        item_loader = response.meta.get('item_loader')
        images = [x.split("(")[1].split(")")[0] for x in sel.xpath("//div[@class='gx-gallery-slide']//@style").getall()]
        item_loader.add_value("images", images)

        bathroom_count = sel.xpath("//ul[@class='gx-labs-list-inline']//li[contains(.,'bagni')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("bagni")[0])
            
        latitude = sel.xpath("//div[@id='gx-map-canvas']//@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)

        longitude = sel.xpath("//div[@id='gx-map-canvas']//@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        utilities = sel.xpath("//li[@class='gx-row-details']//span[contains(@class,'gx-val-details')]//text()[contains(.,'€')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[1])

        energy_label = sel.xpath("(//li[@class='gx-row-details']//label[contains(.,'classe energetica')][1]//following-sibling::span)[1]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        floor = sel.xpath("(//li[@class='gx-row-details']//label[contains(.,'piano')][1]//following-sibling::span)[1]//text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        parking = sel.xpath("(//li[@class='gx-row-details']//label[contains(.,'posti auto')]//following-sibling::span)[1]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)

        elevator = sel.xpath("(//li[@class='gx-row-details']//label[contains(.,'n. ascensori')]//following-sibling::span)[1]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        else:
            item_loader.add_value("elevator", False)

        balcony = sel.xpath("(//li[@class='gx-row-details']//label[contains(.,'balconi')]//following-sibling::span)[1]//text()").get()
        if balcony and "none" not in balcony.lower():
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)

        terrace = sel.xpath("(//li[@class='gx-row-details']//label[contains(.,'terrazzi')]//following-sibling::span)[1]//text()").get()
        if terrace and "none" not in terrace.lower():
            item_loader.add_value("terrace", True)
        else:
            item_loader.add_value("terrace", False)
            
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "loft" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    else:
        return None