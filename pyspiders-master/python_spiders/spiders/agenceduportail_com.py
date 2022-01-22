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
    name = 'agenceduportail_com'
    execution_type='testing'
    country='france'
    locale='fr'
    url = "https://www.agenceduportail.com/catalog/advanced_search_result.php"

    def start_requests(self):
        start_urls = [
            {
                "formdata" : {
                    'aa_afunc': 'call',
                    'aa_sfunc': 'get_products_search_ajax',
                    'aa_cfunc': 'get_scroll_products_callback',
                    'aa_sfunc_args[]': '{"type_page":"carto","infinite":true,"sort":"","page":1,"nb_rows_per_page":6,"C_28_search":"EGAL","C_28_type":"UNIQUE","C_28":"Location","C_27_search":"EGAL","C_27_type":"TEXT","C_27":"1,Loft","C_65_search":"CONTIENT","C_65_type":"TEXT","C_65":"","C_30_MIN":"","C_30_search":"COMPRIS","C_30_type":"NUMBER","C_30_MAX":"","C_34_MIN":"","C_34_search":"COMPRIS","C_34_type":"NUMBER","C_34_MAX":"","C_33_MAX":"","C_38_MAX":"","C_36_MIN":"","C_36_search":"COMPRIS","C_36_type":"NUMBER","C_36_MAX":"","keywords":""}',
                },
                "property_type" : "apartment",
            },
            {
                "formdata" : {
                    'aa_afunc': 'call',
                    'aa_sfunc': 'get_products_search_ajax',
                    'aa_cfunc': 'get_scroll_products_callback',
                    'aa_sfunc_args[]': '{"type_page":"carto","infinite":true,"sort":"","page":1,"nb_rows_per_page":6,"C_28_search":"EGAL","C_28_type":"UNIQUE","C_28":"Location","C_27_search":"EGAL","C_27_type":"TEXT","C_27":"2,17,30,MaisonVille","C_65_search":"CONTIENT","C_65_type":"TEXT","C_65":"","C_30_MIN":"","C_30_search":"COMPRIS","C_30_type":"NUMBER","C_30_MAX":"","C_34_MIN":"","C_34_search":"COMPRIS","C_34_type":"NUMBER","C_34_MAX":"","C_33_MAX":"","C_38_MAX":"","C_36_MIN":"","C_36_search":"COMPRIS","C_36_type":"NUMBER","C_36_MAX":"","keywords":""}',
                },
                "property_type" : "house"
            },
        ]
        for item in start_urls:
            yield FormRequest(self.url,
                            formdata=item["formdata"],
                            dont_filter=True,
                            callback=self.parse,
                            meta={"property_type": item["property_type"], "formdata": item["formdata"]})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        for item in response.xpath("//div[@class='listing-item']/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if page <= 5:
            formdata = response.meta["formdata"]
            formdata["aa_sfunc_args[]"] = formdata["aa_sfunc_args[]"].replace('"page":' + str(page - 1), '"page":' + str(page))
            yield FormRequest(self.url,
                            formdata=formdata,
                            dont_filter=True,
                            callback=self.parse,
                            meta={"property_type": response.meta["property_type"], "formdata": formdata, "page": page + 1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split('?')[0])
        item_loader.add_value("external_source", "Agenceduportail_PySpider_france")
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip(","))
        
        address = response.xpath("//span[@class='ville-title']//text()").get()
        item_loader.add_value("address", address)
        
        city =response.xpath("//li//div[contains(.,'Ville')]/following-sibling::div//text()").get()
        item_loader.add_value("city", city)
        
        zipcode =response.xpath("//li//div[contains(.,'Code')]/following-sibling::div//text()").get()
        item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//li//div[contains(.,'Loyer')]/following-sibling::div//text()").get()
        if rent:
            price = rent.split(" ")[0]
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        utilities = response.xpath("//li//div[contains(.,'sur charges')]/following-sibling::div//text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(" ")[0])
        
        deposit = response.xpath("//li//div[contains(.,'Dépôt')]/following-sibling::div//text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(" ")[0])
        
        square_meters = response.xpath("//li//div[.='Surface']/following-sibling::div//text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//li//div[.='Chambres']/following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li//div[contains(.,'pièces')]/following-sibling::div//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li//div[contains(.,'Salle')]/following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        energy_label = response.xpath("//li//div[.='Conso Energ']/following-sibling::div//text()[not(contains(.,'Vierge'))]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        furnished = response.xpath("//li//div[contains(.,'Meublé')]/following-sibling::div//text()[contains(.,'Oui') or contains(.,'oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//li//div[contains(.,'garage') or contains(.,'parking')]/following-sibling::div//text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//li//div[contains(.,'terrasse')]/following-sibling::div//text()[.!='0']").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//li//div[contains(.,'Balcon') or contains(.,'balcon')]/following-sibling::div//text()[.!='0']").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        elevator = response.xpath("//li//div[contains(.,'Ascenseur')]/following-sibling::div//text()[contains(.,'Oui') or contains(.,'oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        floor = response.xpath("//li//div[.='Etage']/following-sibling::div//text()").get()
        if floor:
            item_loader.add_value("floor", floor)
             
        import dateparser
        available_date = response.xpath("//li//div[contains(.,'Disponibilité')]/following-sibling::div//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        external_id = response.xpath("//div[contains(@class,'product-model') and contains(.,'Référence')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip().split(" ")[-1])
        
        description = " ".join(response.xpath("//div[@class='products-description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='item-slider']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        name = response.xpath("//div[@class='photo-agence']//@alt").get()
        if name:
            item_loader.add_value("landlord_name", name)
        
        phone = response.xpath("//a[@class='contact_nego_tel']//@title").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        else:
            item_loader.add_value("landlord_phone", "04 94 72 64 65")
            
        yield item_loader.load_item()