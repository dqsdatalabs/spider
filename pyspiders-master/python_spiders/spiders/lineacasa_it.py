# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'lineacasa_it' 
    external_source = "Lineacasa_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it'
    url = "https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=73744353-1701-4f92-b6fb-a478d4b7e33c&querystring=%3FfromSearch%3Dtrue%26Contratto%3DA%26Categoria%3D1%26Provincia%3D%26Comune%3D%26Quartiere%3D%26PrezzoMin%3D%26PrezzoMax%3D%26SuperficieMin%3D%26SuperficieMax%3D%26Locali%3D%26Bagni%3D%26Riferimento%3D&clientId=&clientClassName=&numeroColonne=1&lingua=it&numeroRisultati=9&urlPaginaRisultati=&urlPaginaAnnuncio=%2Fscheda-annuncio%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fscheda-cantiere%2F%7Bcomune%7D%2F%7Bidcantiere%7D&mostraTitoli=true&mostraZonaQuartiere=true&mostraDescrizione=false&mostraGallery=false&carousel=false&mostraDettagli=true&mostraBottoni=true&mostraAnnunciSalvati=true&mostraIcone=false&mostraPrivacy=true&urlPrivacy=..%2Fprivacy%2Findex.html&startRisultati=-1&RewriteProvincia=&RewriteComune=&RewriteQuartiere=&RewriteTipologia=&tolerancePrezzo=0&toleranceSuperficie=0&toleranceLocali=false&annunciSalvati=&_=1631184925470"
    
    headers = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
    'Accept': '*/*',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
    'sec-ch-ua-platform': '"Windows"',
    'Origin': 'https://www.lineacasa.it',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://www.lineacasa.it/',
    'Accept-Language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4'
    }
    
    def start_requests(self):
        
        yield Request(
            url=self.url,
            callback=self.parse,
            headers=self.headers,
        )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        sel = Selector(text=json.loads(data)["html"], type="html")
        page = response.meta.get('page', 9)
        seen = False
        for item in sel.xpath("//div[@class='gx-risultato']/a/@href").extract():
            url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/dettaglio?apikey=73744353-1701-4f92-b6fb-a478d4b7e33c&IDImmobile=0&runtimeId=gxAnnuncio-1631189045147303977&pathName={item.split('?fromSearch')[0].replace('/', '%2F')}&clientId=&clientClassName=&lingua=it&urlPaginaAnnuncio=%2Fscheda-annuncio%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fscheda-cantiere%2F%7Bcomune%7D%2F%7Bidcantiere%7D&mostraTitolo=true&mostraBottoneStampa=true&mostraBottoniSocial=true&mostraBottoneInviaAmico=true&mostraDescrizione=true&mostraBottoneLeggiTutto=true&mostraBottoneCantiere=true&mostraMappa=true&mostraDettagli=true&mostraBottoneDettagli=false&mostraGallery=true&carousel=false&thumbnails=bottom&mostraVideo=true&mostraPlanimetrie=true&mostraVirtualTour=true&mostraRichiestaInfo=true&mostraPrivacy=true&urlPrivacy=..%2Fprivacy%2Findex.html&richiestaCampoCustom1=&richiestaCampoCustom2=&richiestaCampoCustom3=&_=1631189043585"
            
            base_url = f"https://www.lineacasa.it{item.split('?fromSearch')[0]}"
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "base_url": base_url})
            seen = True

        if page == 9 or seen:
            url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=73744353-1701-4f92-b6fb-a478d4b7e33c&querystring=%3FfromSearch%3Dtrue%26Contratto%3DA%26Categoria%3D1%26Provincia%3D%26Comune%3D%26Quartiere%3D%26PrezzoMin%3D%26PrezzoMax%3D%26SuperficieMin%3D%26SuperficieMax%3D%26Locali%3D%26Bagni%3D%26Riferimento%3D&clientId=&clientClassName=&numeroColonne=1&lingua=it&numeroRisultati=9&urlPaginaRisultati=&urlPaginaAnnuncio=%2Fscheda-annuncio%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fscheda-cantiere%2F%7Bcomune%7D%2F%7Bidcantiere%7D&mostraTitoli=true&mostraZonaQuartiere=true&mostraDescrizione=false&mostraGallery=false&carousel=false&mostraDettagli=true&mostraBottoni=true&mostraAnnunciSalvati=true&mostraIcone=false&mostraPrivacy=true&urlPrivacy=..%2Fprivacy%2Findex.html&sortBy=&startRisultati={page}"
            yield Request(url, callback=self.parse, headers=self.headers, meta={"page": page+9, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        data = json.loads(response.body.split(b'">')[1].split(b'</')[0].strip())
        sel = Selector(text=data["html"].replace("&lt;","<").replace("&gt;",">"), type="html")
         
        item_loader.add_value("external_link", response.meta.get('base_url'))
        item_loader.add_value("external_id", response.meta.get('base_url').split("/")[-1])
        
        prop_type = sel.xpath("//li[label[contains(.,'tipologia')]]/span/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return
        item_loader.add_value("external_source", self.external_source)

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

            rent = rent.split("€")[1].strip().replace(".","")
            item_loader.add_value("rent", rent)

            deposit = sel.xpath("//li[label[text()='mesi cauzione:']]/span/text()").get()

            if deposit:
                item_loader.add_value("deposit", int(deposit)*int(rent))

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
        
        desc = "".join(sel.xpath("//p[@class='gx-printable']//text()").getall())
        if desc:
            desc=re.sub('\s{2,}', ' ', desc.strip())
            desc=desc.split("Per info tel")[0].split("PER INFORMAZIONI")[0].split("tel.")[0].split("Info telefono")[0].split("Per maggiori Informazioni")[0].split("Per altre informazioni")[0]
            item_loader.add_value("description", desc)
        
        balcony = sel.xpath("//li[label[contains(.,'balcon')]]/span/text()[.!='0']").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = sel.xpath("//li[label[contains(.,'ascensori')]]/span/text()[.!='0']").get()
        if elevator and "no" not in elevator.lower():
            item_loader.add_value("elevator", True)
        parking=sel.xpath("//label[.='box:']/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        furnished=sel.xpath("//label[.='stato arredamento:']/text()").get()
        if furnished:
            item_loader.add_value("furnished",True)
               
        item_loader.add_value("landlord_name", "LINEACASA")
        item_loader.add_value("landlord_phone", "800 199 349")
        item_loader.add_value("landlord_email", "info@lineacasare.it")
        
        url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/dettaglio?apikey=73744353-1701-4f92-b6fb-a478d4b7e33c&IDImmobile=0&runtimeId=gxAnnuncio-1631192284090779543&pathName={response.url.split('pathName=')[1].split('&')[0]}&clientId=&clientClassName=&lingua=it&urlPaginaAnnuncio=%2Fscheda-annuncio%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fscheda-cantiere%2F%7Bcomune%7D%2F%7Bidcantiere%7D&mostraTitolo=true&mostraBottoneStampa=true&mostraBottoniSocial=true&mostraBottoneInviaAmico=true&mostraDescrizione=true&mostraBottoneLeggiTutto=true&mostraBottoneCantiere=true&mostraMappa=true&mostraDettagli=true&mostraBottoneDettagli=false&mostraGallery=true&carousel=false&thumbnails=bottom&mostraVideo=true&mostraPlanimetrie=true&mostraVirtualTour=true&mostraRichiestaInfo=true&mostraPrivacy=true&urlPrivacy=..%2Fprivacy%2Findex.html"
              
        yield Request(url, callback=self.get_images_location, headers=self.headers, meta={"item_loader": item_loader})
    
    def get_images_location(self, response):
        data = json.loads(response.body)
        sel = Selector(text=json.loads(data)["html"], type="html")
        item_loader = response.meta.get('item_loader')
        images = [x.split("(")[1].split(")")[0] for x in sel.xpath("//div[@class='gx-gallery-slide']//@style").getall()]
        item_loader.add_value("images", images)
        
        latitude = sel.xpath("//div[@class='gx-mappa-annuncio']/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = sel.xpath("//div[@class='gx-mappa-annuncio']/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "loft" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    else:
        return None