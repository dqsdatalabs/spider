# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from ..loaders import ListingLoader
import json
import re
from ..helper import *




class AgrealtygroupSpider(scrapy.Spider):
    name = "agrealtygroup"
    start_urls = ['https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=28A2ABB1-04EA-44A8-A257-F3BDCDCDF6B9&querystring=%3FfromSearch%3Dtrue%26Contratto%3DA%26Categoria%3D1%26PrezzoMin%3D%26PrezzoMax%3D%26SuperficieMin%3D%26SuperficieMax%3D%26Locali%3D%26Vani%3D%26Bagni%3D%26Riferimento%3D%26ClasseEnergetica%3D&clientId=&clientClassName=&numeroColonne=3&lingua=it&numeroRisultati=5&urlPaginaRisultati=%2Fit%2Fannunci-immobiliari&urlPaginaAnnuncio=%2Fit%2Fimmobile%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fit%2Fcantiere%2F%7Bidcantiere%7D&mostraTitoli=true&mostraZonaQuartiere=true&mostraDescrizione=true&mostraGallery=true&carousel=true&mostraDettagli=true&mostraBottoni=true&mostraAnnunciSalvati=true&mostraIcone=true&mostraPrivacy=true&urlPrivacy=%2Fit%2Fprivacy&startRisultati=-1&RewriteProvincia=&RewriteComune=&RewriteQuartiere=&RewriteTipologia=&tolerancePrezzo=0&toleranceSuperficie=0&toleranceLocali=false&annunciSalvati=&_=1638116871261']
    allowed_domains = ["www.agrealtygroup.it"]
    country = 'italy' # Fill in the Country's name
    locale = "it" # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1
    page=True
    num=0

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2

    def parse(self, response, **kwargs):
        try:
            data = json.loads(response.body)
            data = json.loads(data)["html"]
        except:
            data = response.body.split(b">")[1].split(b"<")[0].strip()
            data = json.loads(data)["html"].replace("&lt;", "<").replace("&gt;", ">")
        sel = Selector(text=data, type="html")
        seen = False
        for item in sel.xpath("//div[@class='gx-risultato']"):
            item_loader = ListingLoader(response=response)
            external_id = item.xpath("./a/@href").get()
            if external_id:
                external_it = external_id.split("/")[7].split("?")[0]
                item_loader.add_value("external_id", external_it)
            external_id = external_id.split("?")[0]
            item_loader.add_value("external_link", "https://www.agrealtygroup.it"+external_id+f"?fromSearch=true&Contratto=A&Categoria=1&PrezzoMin=&PrezzoMax=&SuperficieMin=&SuperficieMax=&Locali=&Vani=&Bagni=&Riferimento=&ClasseEnergetica=&StartRisultati={self.num}")
            item_loader.add_value("external_source", self.external_source)
            prop_type = item.xpath(".//h2/text()").get()
            if prop_type and ("appartament" in prop_type.lower()):
                item_loader.add_value("property_type", "apartment")
            elif prop_type and ("house" in prop_type.lower() or "loft" in prop_type.lower() or "attico" in prop_type.lower()):
                item_loader.add_value("property_type", "house")
            else :
                continue
            title = item.xpath(".//h2/text()").get()
            # print(title)
            item_loader.add_value("title", title)
            #
            # address = item.xpath(".//div[@class='gx-risultato-testo']/h3/text()").get()
            # if address:
            #     item_loader.add_value("address", address)

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

            item_loader.add_value("landlord_name", "AG Realty Group S.r.l.")
            item_loader.add_value("landlord_phone", "3397653894")
            item_loader.add_value("landlord_email", "aleperina@libero.it")
            url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/dettaglio?apikey=28A2ABB1-04EA-44A8-A257-F3BDCDCDF6B9&IDImmobile=0&runtimeId=gxAnnuncio-1638118871734730077&pathName={external_id}&clientId=&clientClassName=&lingua=it&urlPaginaAnnuncio=/it/immobile/"+"{categoria}/{contratto}/{tipologia}/{comune}-{siglaprovincia}/{idimmobile}"+"&urlPaginaCantiere=/it/cantiere/{idcantiere}&mostraTitolo=true&mostraBottoneStampa=true&mostraBottoniSocial=true&mostraBottoneInviaAmico=true&mostraDescrizione=true&mostraBottoneLeggiTutto=true&mostraBottoneCantiere=true&mostraMappa=true&mostraDettagli=true&mostraBottoneDettagli=false&mostraIconaAsta=false&mostraGallery=true&carousel=true&thumbnails=bottom&mostraVideo=true&mostraPlanimetrie=true&mostraVirtualTour=true&mostraRichiestaInfo=true&mostraPrivacy=true&urlPrivacy=/it/privacy&_=1638118871469"
            yield Request(url, callback=self.populate_item, meta={"item_loader": item_loader},dont_filter=True)


        if self.page:
            url = f"https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=28A2ABB1-04EA-44A8-A257-F3BDCDCDF6B9&querystring=%3FfromSearch%3Dtrue%26Contratto%3DA%26Categoria%3D1%26PrezzoMin%3D%26PrezzoMax%3D%26SuperficieMin%3D%26SuperficieMax%3D%26Locali%3D%26Vani%3D%26Bagni%3D%26Riferimento%3D%26ClasseEnergetica%3D%26StartRisultati%3D5&clientId=&clientClassName=&numeroColonne=3&lingua=it&numeroRisultati=5&urlPaginaRisultati=%2Fit%2Fannunci-immobiliari&urlPaginaAnnuncio=%2Fit%2Fimmobile%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fit%2Fcantiere%2F%7Bidcantiere%7D&mostraTitoli=true&mostraZonaQuartiere=true&mostraDescrizione=true&mostraGallery=true&carousel=true&mostraDettagli=true&mostraBottoni=true&mostraAnnunciSalvati=true&mostraIcone=true&mostraPrivacy=true&urlPrivacy=%2Fit%2Fprivacy&startRisultati=-1&RewriteProvincia=&RewriteComune=&RewriteQuartiere=&RewriteTipologia=&tolerancePrezzo=0&toleranceSuperficie=0&toleranceLocali=false&annunciSalvati=&_=1638127513772"
            self.page=False
            self.num=5
            yield Request(url, callback=self.parse,dont_filter=True)

    def populate_item(self, response):
        data = response.body.split(b">")[1].split(b"<")[0].strip()
        data = json.loads(data)["html"].replace("&lt;", "<").replace("&gt;", ">")
        sel = Selector(text=data, type="html")
        item_loader = response.meta.get('item_loader')
        images = [x.split("(")[1].split(")")[0] for x in sel.xpath("//div[@class='gx-owl-gallery-image-item']//@style").getall()]
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        bathroom_count = sel.xpath("//ul[@class='gx-labs-list-inline']//li[contains(.,'bagni')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("bagni")[0])

        latitude = sel.xpath("//div[@id='gx-map-canvas']//@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)

        longitude = sel.xpath("//div[@id='gx-map-canvas']//@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        zipcode, city, address=extract_location_from_coordinates(float(longitude),float(latitude))
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
        item_loader.add_value("address", address)

        utilities = sel.xpath(
            "//li[@class='gx-row-details']//span[contains(@class,'gx-val-details')]//text()[contains(.,'€')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[1])

        energy_label = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'classe energetica')][1]//following-sibling::span)[1]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        floor = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'piano')][1]//following-sibling::span)[1]//text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        parking = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'posti auto')]//following-sibling::span)[1]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)

        elevator = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'n. ascensori')]//following-sibling::span)[1]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        else:
            item_loader.add_value("elevator", False)

        balcony = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'balconi')]//following-sibling::span)[1]//text()").get()
        if balcony and "none" not in balcony.lower():
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)

        terrace = sel.xpath(
            "(//li[@class='gx-row-details']//label[contains(.,'terrazzi')]//following-sibling::span)[1]//text()").get()
        if terrace and "none" not in terrace.lower():
            item_loader.add_value("terrace", True)
        else:
            item_loader.add_value("terrace", False)
        item_loader.add_value("position", self.position)
        self.position+=1

        yield item_loader.load_item()

