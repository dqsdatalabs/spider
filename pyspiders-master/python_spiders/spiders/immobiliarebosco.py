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
    name = 'immobiliarebosco'
    external_source = "Immobiliarebosco_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it'
    url = "https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=3907417C-66BE-4ED0-8283-AA4C253906D1&querystring=%3FfromSearch%3Dtrue%26Contratto%3DA%26Categoria%3D1%26PrezzoMin%3D%26PrezzoMax%3D%26SuperficieMin%3D%26SuperficieMax%3D&clientId=&clientClassName=&numeroColonne=3&lingua=it&numeroRisultati=5&urlPaginaRisultati=%2Fit%2Fannunci-immobiliari&urlPaginaAnnuncio=%2Fit%2Fimmobile%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fit%2Fcantiere%2F%7Bidcantiere%7D&mostraTitoli=true&mostraZonaQuartiere=true&mostraDescrizione=true&mostraGallery=true&carousel=true&mostraDettagli=true&mostraBottoni=true&mostraAnnunciSalvati=true&mostraIcone=true&mostraPrivacy=true&urlPrivacy=%2Fit%2Fprivacy&startRisultati=-1&RewriteProvincia=&RewriteComune=&RewriteQuartiere=&RewriteTipologia=&tolerancePrezzo=0&toleranceSuperficie=0&toleranceLocali=false&annunciSalvati=&_=1631597497172"
    
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'Origin': 'http://www.immobiliarebosco.net',
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
        page = response.meta.get('page', 5)
        
        seen = False
        for item in sel.xpath("//div[@class='gx-risultato']/a/@href").extract():
            url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/dettaglio?apikey=3907417C-66BE-4ED0-8283-AA4C253906D1&IDImmobile=0&runtimeId=gxAnnuncio-1631598257022875674&pathName={item.split('?fromSearch')[0].replace('/', '%2F')}&clientId=&clientClassName=&lingua=it&urlPaginaAnnuncio=%2Fit%2Fimmobile%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fit%2Fcantiere%2F%7Bidcantiere%7D&mostraTitolo=true&mostraBottoneStampa=true&mostraBottoniSocial=true&mostraBottoneInviaAmico=true&mostraDescrizione=true&mostraBottoneLeggiTutto=true&mostraBottoneCantiere=true&mostraMappa=true&mostraDettagli=true&mostraBottoneDettagli=false&mostraGallery=true"
            base_url = f"http://www.immobiliarebosco.net{item.split('?fromSearch')[0]}"
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "base_url": base_url})
            seen = True
        
        if page == 5 or seen:
            url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=3907417C-66BE-4ED0-8283-AA4C253906D1&querystring=%3FfromSearch%3Dtrue%26Contratto%3DA%26Categoria%3D1%26PrezzoMin%3D%26PrezzoMax%3D%26SuperficieMin%3D%26SuperficieMax%3D%26StartRisultati%3D{page}&clientId=&clientClassName=&numeroColonne=3&lingua=it&numeroRisultati=5&urlPaginaRisultati=%2Fit%2Fannunci-immobiliari&urlPaginaAnnuncio=%2Fit%2Fimmobile%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fit%2Fcantiere%2F%7Bidcantiere%7D&mostraTitoli=true&mostraZonaQuartiere=true&mostraDescrizione=true&mostraGallery=true&carousel=true&mostraDettagli=true&mostraBottoni=true&mostraAnnunciSalvati=true&mostraIcone=true&mostraPrivacy=true&urlPrivacy=%2Fit%2Fprivacy&startRisultati=-1&RewriteProvincia=&RewriteComune=&RewriteQuartiere=&RewriteTipologia=&tolerancePrezzo=0"
            yield Request(url, callback=self.parse, meta={"page": page+5, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        data = json.loads(response.body.split(b'">')[1].split(b'</')[0].strip())

        sel = Selector(text=data["html"].replace("&lt;","<").replace("&gt;",">"), type="html")    
        item_loader.add_value("external_link", response.meta.get('base_url'))
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("external_id", response.meta.get('base_url').split("/")[-1])
        
        prop_type = sel.xpath("//li[label[contains(.,'tipologia')]]/span/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return
        
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
        if rent:
            item_loader.add_value("rent", rent.split("???")[1].strip())
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
            utilities = utilities.split(",")[0].split("???")[1].strip().replace(".","")
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
        
        parking = sel.xpath("//li[label[contains(.,'auto')]]/span/text()[.!='assente']").get()
        if parking:
            item_loader.add_value("parking",True)

        latitude = sel.xpath("//./@data-lat").get()
        if latitude:
            item_loader.add_value("latitude",latitude)

        longitude = sel.xpath("//./@data-lng").get()
        if longitude:
            item_loader.add_value("longitude",longitude)


        item_loader.add_value("landlord_name", "Immobiliare Bosco")
        item_loader.add_value("landlord_phone", "333.73.09.206")
        item_loader.add_value("landlord_email", "info@immobiliarebosco.net")
        
        url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/dettaglio?apikey=3907417C-66BE-4ED0-8283-AA4C253906D1&IDImmobile=0&runtimeId=gxAnnuncio-1631600858027764994&pathName={response.url.split('pathName=')[1].split('&')[0]}&clientId=&clientClassName=&lingua=it&urlPaginaAnnuncio=%2Fit%2Fimmobile%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fit%2Fcantiere%2F%7Bidcantiere%7D&mostraTitolo=true&mostraBottoneStampa=true&mostraBottoniSocial=true&mostraBottoneInviaAmico=true&mostraDescrizione=true&mostraBottoneLeggiTutto=true&mostraBottoneCantiere=true&mostraMappa=true&mostraDettagli=true&mostraBottoneDettagli=false&mostraGallery=true&carousel=true&thumbnails=bottom&mostraVideo=true&mostraPlanimetrie=true&mostraVirtualTour=true&mostraRichiestaInfo=true&mostraPrivacy=true&urlPrivacy=%2Fit%2Fprivacy&richiestaCampoCustom1=&richiestaCampoCustom2=&richiestaCampoCustom3=&_=1631600856335"
        yield Request(url, callback=self.get_images, headers=self.headers, meta={"item_loader": item_loader})

    def get_images(self, response):
        data = json.loads(response.body)
        
        sel = Selector(text=json.loads(data)["html"], type="html")
        item_loader = response.meta.get('item_loader')
        images = [x.split("(")[1].split(")")[0] for x in sel.xpath("//div[contains(@class,'gx-printable-img gx-owl-item ')]//@style").getall()]
        item_loader.add_value("images", images)
        
        yield item_loader.load_item()
    
def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "loft" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    else:
        return None