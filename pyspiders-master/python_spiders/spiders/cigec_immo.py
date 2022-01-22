# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re
import dateparser
from datetime import datetime

class MySpider(Spider):
    name = 'cigec_immo'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {"HTTPCACHE_ENABLED": False}
    index = 1
    url = "https://cigec.immo/louer.html"
    formdata_apt = {
        '__EVENTTARGET': 'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$BouRechercher',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': '',
        '__VIEWSTATEGENERATOR': '',
        '__SCROLLPOSITIONX': '0',
        '__SCROLLPOSITIONY': '0',
        '__EVENTVALIDATION': '',
        'ctl00$HidContenu': '100%',
        'ctl00$HidSeparateur': '2%',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$HidIdBloc': '55',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl01$HidIdCategorie': '1',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl01$CheCategorie': 'on',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl02$HidIdCategorie': '3',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl03$HidIdCategorie': '5',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl04$HidIdCategorie': '6',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl05$HidIdCategorie': '10',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl06$HidIdCategorie': '11',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl07$HidIdCategorie': '13',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$ChePiece1': 'on',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$ChePiece2': 'on',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$ChePiece3': 'on',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$ChePiece4': 'on',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$ChePiece5': 'on',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$TexBudget': '0+-+€+3000+€',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$LstSecteurs': '',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$TextReference': '',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl02$WYSIWYG$HidIdBloc': '80',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl01$RepColonnes$ctl00$Catalogue$HidIdBloc': '2',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl01$RepColonnes$ctl00$Catalogue$HidMotsCles': '',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl01$RepColonnes$ctl00$Catalogue$PagArticles$HidAleatoire': '',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl01$RepColonnes$ctl00$Catalogue$PagArticles$HidRepeteur': 'RepArticles',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl01$RepColonnes$ctl00$Catalogue$PagArticles$LstPages': '0'
    }
    formdata_hse = {
        '__EVENTTARGET': 'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$BouRechercher',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': '',
        '__VIEWSTATEGENERATOR': '',
        '__SCROLLPOSITIONX': '0',
        '__SCROLLPOSITIONY': '0',
        '__EVENTVALIDATION': '',
        'ctl00$HidContenu': '100%',
        'ctl00$HidSeparateur': '2%',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$HidIdBloc': '55',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl01$HidIdCategorie': '1',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl02$HidIdCategorie': '3',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl02$CheCategorie': 'on',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl03$HidIdCategorie': '5',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl04$HidIdCategorie': '6',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl05$HidIdCategorie': '10',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl06$HidIdCategorie': '11',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$RepCategories$ctl07$HidIdCategorie': '13',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$ChePiece1': 'on',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$ChePiece2': 'on',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$ChePiece3': 'on',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$ChePiece4': 'on',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$ChePiece5': 'on',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$TexBudget': '0+-+€+3000+€',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$LstSecteurs': '',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl00$Filtres$TextReference': '',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl00$RepColonnes$ctl02$WYSIWYG$HidIdBloc': '80',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl01$RepColonnes$ctl00$Catalogue$HidIdBloc': '2',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl01$RepColonnes$ctl00$Catalogue$HidMotsCles': '',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl01$RepColonnes$ctl00$Catalogue$PagArticles$HidAleatoire': '',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl01$RepColonnes$ctl00$Catalogue$PagArticles$HidRepeteur': 'RepArticles',
        'ctl00$CPHContenu$ctl00$RepLignes$ctl01$RepColonnes$ctl00$Catalogue$PagArticles$LstPages': '0'
    }
    headers = {
        'authority': 'cigec.immo',
        'cache-control': 'max-age=0',
        'upgrade-insecure-requests': '1',
        'origin': 'https://cigec.immo',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'referer': 'https://cigec.immo/louer.html',
        'accept-language': 'tr,en;q=0.9',
        'Cookie': 'ASP.NET_SessionId=uh4gomaeurtyqvagenc5kc0d'
    }
    
    # 1. FOLLOWING
    def start_requests(self):
        yield Request("https://cigec.immo/louer.html", callback=self.parse)

    def parse(self, response):

        self.formdata_apt["__VIEWSTATE"] = response.xpath("//input[@id='__VIEWSTATE']/@value").get()
        self.formdata_apt["__VIEWSTATEGENERATOR"] = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").get()
        self.formdata_apt["__EVENTVALIDATION"] = response.xpath("//input[@id='__EVENTVALIDATION']/@value").get()
        self.formdata_hse["__VIEWSTATE"] = response.xpath("//input[@id='__VIEWSTATE']/@value").get()
        self.formdata_hse["__VIEWSTATEGENERATOR"] = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").get()
        self.formdata_hse["__EVENTVALIDATION"] = response.xpath("//input[@id='__EVENTVALIDATION']/@value").get()

        yield FormRequest(self.url, headers=self.headers, formdata=self.formdata_apt, dont_filter=True, callback=self.jump, meta={"property_type": "apartment", "cat": "1"})
    
    def jump(self, response):

        page = response.meta.get("page", 1)
        seen = False
        
        for item in response.xpath("//a[contains(.,'Détails')]/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={'property_type': response.meta["property_type"]})
        
        validation = response.xpath("//input[@id='__EVENTVALIDATION']/@value").get()
        viewstate = response.xpath("//input[@id='__VIEWSTATE']/@value").get()
        generator = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").get()
        hid_repeteur = response.xpath("//input[@id='CPHContenu_Catalogue_PagArticles_HidAleatoire']/@value").get()

        url = "https://cigec.immo/recherche.html"
        formdata = {
            '__EVENTTARGET': 'ctl00$CPHContenu$Catalogue$PagArticles$LstPages',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            '__VIEWSTATE': f"{viewstate}",
            '__VIEWSTATEGENERATOR': f"{generator}",
            '__SCROLLPOSITIONX': '0',
            '__SCROLLPOSITIONY': '273',
            '__EVENTVALIDATION': f"{validation}",
            'ctl00$HidContenu': '100%',
            'ctl00$HidSeparateur': '2%',
            'ctl00$CPHContenu$Catalogue$HidIdBloc': '',
            'ctl00$CPHContenu$Catalogue$HidMotsCles': f'Categories:{response.meta["cat"]};Pieces:Tout;Budget:0+-+€+3000+€;Secteur:;Reference:;',
            'ctl00$CPHContenu$Catalogue$PagArticles$HidAleatoire': f"{hid_repeteur}",
            'ctl00$CPHContenu$Catalogue$PagArticles$HidRepeteur': 'RepArticles',
            'ctl00$CPHContenu$Catalogue$PagArticles$LstPages': f"{page}"
        }
        headers = {
            'authority': 'cigec.immo',
            'cache-control': 'max-age=0',
            'upgrade-insecure-requests': '1',
            'origin': 'https://cigec.immo',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'referer': 'https://cigec.immo/recherche.html',
            'accept-language': 'tr,en;q=0.9',
            'Cookie': 'ASP.NET_SessionId=uh4gomaeurtyqvagenc5kc0d'
        }

        if page == 1 or seen:
            yield FormRequest(url, callback=self.jump, headers=headers, formdata=formdata, dont_filter=True, meta={"page": page + 1, "property_type": response.meta["property_type"], "cat": response.meta["cat"]})
        if self.index <2:
            yield FormRequest(self.url, headers=self.headers, formdata=self.formdata_hse, dont_filter=True, callback=self.jump, meta={"property_type": "house", "cat": "3"})
            self.index += 1
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Cigecimmo_PySpider_" + self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//div[@class='CadreSolo']/h2/text()")
        rent = response.xpath("//p[@class='Centre']/span[@class='PrixMaxi']/text()").extract_first().replace(",",".")
        if rent:
            price = rent.split("€")[0]
            item_loader.add_value("rent", int(float(price.replace("\xa0",""))))
        item_loader.add_value("currency", "EUR")
        
        square_meters =  response.xpath("//div[@class='CadreSolo']/p[contains(.,'Surface habitable : ')]/span/text()").extract_first()
        if square_meters:
            sq = square_meters.replace(",",".")
            item_loader.add_value("square_meters", int(float(sq)))
        item_loader.add_xpath("bathroom_count", "//div[@class='CadreSolo']/p[contains(.,'Nb de salle d')]/span/text()")        
        item_loader.add_xpath("room_count", "//div[@class='CadreSolo']/p[contains(.,'Nb de pièce : ')]/span/text()")
        item_loader.add_xpath("zipcode", "//div[@class='CadreSolo']/p[contains(.,'Code postal : ')]/span/text()")

        item_loader.add_xpath("utilities", "//div[@class='CadreSolo']/p[contains(.,'Charge :')]/span/text()")
        item_loader.add_xpath("deposit", "//div[@class='CadreSolo']/p[contains(.,'Dépot de garantie :')]/span/text()")
        
        item_loader.add_xpath("energy_label", "//div[@class='CadreSolo']/p[contains(.,'Bilan consommation ')]/span/text()")
        item_loader.add_xpath("address", "//div[@class='CadreSolo']/p[contains(.,'Ville :')]/span/text()")

        external_id = response.xpath("//div[@class='CadreSolo']/p[contains(.,'Référence')]/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[1].strip() )

        desc = " ".join(response.xpath("//div[@class='CadreSolo']/p[5]/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@id='Galerie_CPHContenu_Galerie']/a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images)   

        item_loader.add_xpath("city", "//div[@class='CadreSolo']/p[contains(.,'Ville :')]/span/text()")
        parking =  response.xpath("//div[@class='CadreSolo']/p[contains(.,'Nb de parking :')]/span/text()[ .!='0']").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)

        elevator = response.xpath("//div[@class='CadreSolo']/p[contains(.,'Ascenseur')]/span/text()").get()
        if elevator:
            if "OUI" in elevator:
                item_loader.add_value("elevator", True)
            if "NON" in elevator:
                item_loader.add_value("elevator", False)
        
        floor = response.xpath("//div[@class='CadreSolo']/p[contains(.,'étage')]/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        bathroom_count = response.xpath("//div[@class='CadreSolo']/p[contains(.,'salle de')]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        available_date = response.xpath("//div[@class='CadreSolo']/p[5]/text()[contains(.,'Disponible')]").get()    
        if available_date:     
            available_date = available_date.lower().split("disponible")[1]   
            if "immédiatement" in available_date or "de suite" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.replace(" le ","").strip(), date_formats=["%d/%m/%Y"], languages=['fr'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)        
        
        item_loader.add_value("landlord_email", "cigec@wanadoo.fr")
        item_loader.add_value("landlord_name", "Cigec immo")
        item_loader.add_value("landlord_phone", "+33 (0)5 61 25 35 25")
        
        yield item_loader.load_item()