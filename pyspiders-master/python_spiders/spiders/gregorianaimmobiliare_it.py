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
    name = 'gregorianaimmobiliare_it'
    external_source = "Gregorianaimmobiliare_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it'
    
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'Origin': 'https://www.gregorianaimmobiliare.it',
        'Accept-Language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4'
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=5CA710F3-8FBB-495F-9452-5D05F8040ED2&querystring=%3FfromSearch%3Dtrue%26Contratto%3DA%26Tipologia%3D1%26Categoria%3D1%26Provincia%3D%26Comune%3D%26Zona%3D%26PrezzoMin%3D%26PrezzoMax%3D%26SuperficieMin%3D%26SuperficieMax%3D%26Locali%3D%26Vani%3D%26Bagni%3D%26Riferimento%3D%26ClasseEnergetica%3D&clientId=&clientClassName=&numeroColonne=2&lingua=it&numeroRisultati=6&urlPaginaRisultati=%2Fit%2Fannunci-immobiliari&urlPaginaAnnuncio=%2Fit%2Fimmobile%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fit%2Fcantiere%2F%7Bidcantiere%7D",
                    "https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=5CA710F3-8FBB-495F-9452-5D05F8040ED2&querystring=%3FfromSearch%3Dtrue%26Contratto%3DA%26Tipologia%3D2%26Categoria%3D1%26Provincia%3D%26Comune%3D%26Zona%3D%26PrezzoMin%3D%26PrezzoMax%3D%26SuperficieMin%3D%26SuperficieMax%3D%26Locali%3D%26Vani%3D%26Bagni%3D%26Riferimento%3D%26ClasseEnergetica%3D&clientId=&clientClassName=&numeroColonne=2&lingua=it&numeroRisultati=6&urlPaginaRisultati=%2Fit%2Fannunci-immobiliari&urlPaginaAnnuncio=%2Fit%2Fimmobile%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fit%2Fcantiere%2F%7Bidcantiere%7D"
                    
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=5CA710F3-8FBB-495F-9452-5D05F8040ED2&querystring=%3FfromSearch%3Dtrue%26Contratto%3DA%26Tipologia%3D10%26Categoria%3D1%26Provincia%3D%26Comune%3D%26Zona%3D%26PrezzoMin%3D%26PrezzoMax%3D%26SuperficieMin%3D%26SuperficieMax%3D%26Locali%3D%26Vani%3D%26Bagni%3D%26Riferimento%3D%26ClasseEnergetica%3D&clientId=&clientClassName=&numeroColonne=2&lingua=it&numeroRisultati=6&urlPaginaRisultati=%2Fit%2Fannunci-immobiliari&urlPaginaAnnuncio=%2Fit%2Fimmobile%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fit%2Fcantiere%2F%7Bidcantiere%7D"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    headers=self.headers,
                    meta={'property_type': url.get('property_type')}
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
        
        for item in sel.xpath("//div[@class='gx-risultato']/a/@href").extract():
            url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/dettaglio?apikey=5CA710F3-8FBB-495F-9452-5D05F8040ED2&IDImmobile=0&runtimeId=gxAnnuncio-1631530445525716679&pathName={item.split('?fromSearch')[0].replace('/', '%2F')}&clientId=&clientClassName=&lingua=it&urlPaginaAnnuncio=%2Fit%2Fimmobile%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fit%2Fcantiere%2F%7Bidcantiere%7D&mostraTitolo=true&mostraBottoneStampa=true&mostraBottoniSocial=true&mostraBottoneInviaAmico=true&mostraDescrizione=true&mostraBottoneLeggiTutto=false&mostraBottoneCantiere=true&mostraMappa=true&mostraDettagli=true&mostraBottoneDettagli=false&mostraGallery=true&carousel=false&thumbnails=bottom&mostraVideo=true&mostraPlanimetrie=true&mostraVirtualTour=true&mostraRichiestaInfo=true&mostraPrivacy=true&urlPrivacy=%2Fit%2Fprivacy&richiestaCampoCustom1=&richiestaCampoCustom2=&richiestaCampoCustom3=&_=1631530444863"
            base_url = f"https://www.gregorianaimmobiliare.it{item.split('?fromSearch')[0]}"
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "base_url": base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        data = json.loads(response.body.split(b'">')[1].split(b'</')[0].strip())
        sel = Selector(text=data["html"].replace("&lt;","<").replace("&gt;",">"), type="html")

        status= sel.xpath("//h1/text()").get()
        if "affittato" not in status.lower():
            
            item_loader.add_value("external_link", response.meta.get('base_url'))
            item_loader.add_value("external_source", self.external_source)

            item_loader.add_value("external_id", response.meta.get('base_url').split("/")[-1])
            
            item_loader.add_value("property_type", response.meta.get('property_type'))
            
            title = sel.xpath("//h1/text()").get()
            item_loader.add_value("title", title)

            address = sel.xpath("//h3[contains(@class,'gx-printable-indirizzo')]/text()").get()
            if address:
                address = address.split(":")[1].replace("via ","").replace("Via ","").strip()
                item_loader.add_value("address", address)
                
                city = ""
                for i in address.split(" "):
                    if not i.isdigit():
                        city += i+" "
                if city:
                    item_loader.add_value("city", city.strip())
                        
            rent = sel.xpath("//li[span[contains(.,'Prezzo')]]/span[2]/text()").get()
            if rent and "€" in rent:
                rent=rent.split("€")[1].strip()
                rent=rent.replace(".","")
                item_loader.add_value("rent", rent)
                deposit=sel.xpath("//li[@class='gx-row-details']//label[contains(.,'cauzione')]//following-sibling::span//text()[1]").get()
                if deposit:
                    deposit=int(float(rent)*float(deposit))
                    item_loader.add_value("deposit", deposit)
            item_loader.add_value("currency", "EUR")

            
           

            room_count = sel.xpath("//li[span[contains(.,'Locali')]]/span[2]/text()").get()
            item_loader.add_value("room_count", room_count)

            bathroom_count = sel.xpath("//li[label[contains(.,'bagni')]]/span/text()").get()
            item_loader.add_value("bathroom_count", bathroom_count)
            
            square_meters = sel.xpath("//li[span[contains(.,'Superficie')]]/span[2]/text()").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.split("m")[0].strip())

            utilities = sel.xpath("//li[label[contains(.,'spese')]]/span/text()").get()
            if utilities:
                utilities = utilities.split(",")[0].split("€")[1].strip().replace(".","")
                item_loader.add_value("utilities", utilities)
                
            floor = sel.xpath("//li[label[contains(.,'piani')]]/span/text()").get()
            item_loader.add_value("floor", floor)
            
            energy_label = sel.xpath("//li[label[.='classe energetica:']]/span/text()").get()
            if energy_label:
                item_loader.add_value("energy_label", energy_label)
            
            desc = "".join(sel.xpath("//div[contains(@class,'descrizione')]//text()").getall())
            if desc:
                item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
            
            balcony = sel.xpath("//li[label[contains(.,'balcon')]]/span/text()[.!='0']").get()
            if balcony:
                item_loader.add_value("balcony", True)
            
            elevator = sel.xpath("//li[label[contains(.,'ascensori')]]/span/text()[.!='0']").get()
            if elevator and "no" not in elevator.lower():
                item_loader.add_value("elevator", True)

            
            item_loader.add_value("landlord_name", "Gregoriana Immobiliare")
            item_loader.add_value("landlord_phone", "+39 06 944 43 045")
            
            url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/dettaglio?apikey=5CA710F3-8FBB-495F-9452-5D05F8040ED2&IDImmobile=0&runtimeId=gxAnnuncio-1631530445525716679&pathName={response.url.split('pathName=')[1].split('&')[0]}&clientId=&clientClassName=&lingua=it&urlPaginaAnnuncio=%2Fit%2Fimmobile%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fit%2Fcantiere%2F%7Bidcantiere%7D&mostraTitolo=true&mostraBottoneStampa=true&mostraBottoniSocial=true&mostraBottoneInviaAmico=true&mostraDescrizione=true&mostraBottoneLeggiTutto=false&mostraBottoneCantiere=true&mostraMappa=true&mostraDettagli=true&mostraBottoneDettagli=false&mostraGallery=true&carousel=false&thumbnails=bottom&mostraVideo=true&mostraPlanimetrie=true&mostraVirtualTour=true&mostraRichiestaInfo=true"
            yield Request(url, callback=self.get_images, headers=self.headers, meta={"item_loader": item_loader})

    def get_images(self, response):
        data = json.loads(response.body)
        sel = Selector(text=json.loads(data)["html"], type="html")
        item_loader = response.meta.get('item_loader')
        images = [x.split("(")[1].split(")")[0] for x in sel.xpath("//div[@class='gx-gallery-slide']//@style").getall()]
        item_loader.add_value("images", images)
        
        yield item_loader.load_item()