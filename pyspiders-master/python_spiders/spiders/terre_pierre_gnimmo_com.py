# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'terre_pierre_gnimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://terre-pierre.gnimmo.com/catalog/advanced_search_result.php?action=update_search&search_id=1687315911251860&map_polygone=&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_34=0&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MAX=&C_33_MIN=0&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=&C_38_MIN=0&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_36_MIN=&C_2038_temp=0&C_2038=&C_2038_search=EGAL&C_2038_type=FLAG&keywords=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://terre-pierre.gnimmo.com/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2CHotel_Part%2C30%2CMaisonVille%2CMaisonPays&C_27_tmp=2&C_27_tmp=Hotel_Part&C_27_tmp=30&C_27_tmp=MaisonVille&C_27_tmp=MaisonPays&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_34=0&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MAX=&C_33_MIN=0&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=&C_38_MIN=0&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_36_MIN=&C_2038_temp=on&C_2038=&C_2038_search=EGAL&C_2038_type=FLAG&keywords=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)

        if not response.meta.get("max_page"):
            item_count = response.xpath("//div[@class='barre-navigation-top']/div[@class='row-results']/text()").get().split(" ")[0]            
            max_page = int(int(item_count.strip()) / 6) + 1 
            print("----------",max_page)
        else:
            max_page = response.meta.get("max_page")

        seen = False
        for item in response.xpath("//div[@class='cell-product']/a"):
            status = item.xpath(".//div[@class='product-bottom-infos']/div[@class='product-price']/text()").get()
            if status and "loyer" not in status.lower():
                continue 
            follow_url = response.urljoin(item.xpath("./@href").get()).split("?")[0]
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True

        if page < max_page:
            if response.meta["property_type"] == "apartment":
                formdata = {
                    'aa_afunc': 'call',
                    'aa_sfunc': 'get_products_search_ajax',
                    'aa_cfunc': 'get_scroll_products_callback',
                    'aa_sfunc_args[]': '{"type_page":"carto","infinite":true,"sort":"","page":'+str(page)+',"nb_rows_per_page":6,"search_id":1687315911251860,"C_28":"Location","C_28_search":"EGAL","C_28_type":"UNIQUE","C_27_search":"EGAL","C_27_type":"TEXT","C_27":"1","C_65_search":"CONTIENT","C_65_type":"TEXT","C_65":"","C_64_search":"INFERIEUR","C_64_type":"TEXT","C_64":"","C_34":"0","C_34_search":"COMPRIS","C_34_type":"NUMBER","C_34_MIN":"","C_34_MAX":"","C_30_search":"COMPRIS","C_30_type":"NUMBER","C_30_MIN":"","C_30_MAX":"","C_33_search":"COMPRIS","C_33_type":"NUMBER","C_33_MAX":"","C_33_MIN":"0","C_38_search":"COMPRIS","C_38_type":"NUMBER","C_38_MAX":"","C_38_MIN":"0","C_36_search":"COMPRIS","C_36_type":"NUMBER","C_36_MAX":"","C_36_MIN":"","C_2038":"","C_2038_search":"EGAL","C_2038_type":"FLAG","keywords":""}',
                }
            else:
                formdata = {
                    'aa_afunc': 'call',
                    'aa_sfunc': 'get_products_search_ajax',
                    'aa_cfunc': 'get_scroll_products_callback',
                    'aa_sfunc_args[]': '{"type_page":"carto","infinite":true,"sort":"","page":' +str(page)+ ',"nb_rows_per_page":6,"C_28":"Location","C_28_search":"EGAL","C_28_type":"UNIQUE","C_27_search":"EGAL","C_27_type":"TEXT","C_27":"2,Hotel_Part,30,MaisonVille,MaisonPays","C_65_search":"CONTIENT","C_65_type":"TEXT","C_65":"","C_64_search":"INFERIEUR","C_64_type":"TEXT","C_64":"","C_34":"0","C_34_search":"COMPRIS","C_34_type":"NUMBER","C_34_MIN":"","C_34_MAX":"","C_30_search":"COMPRIS","C_30_type":"NUMBER","C_30_MIN":"","C_30_MAX":"","C_33_search":"COMPRIS","C_33_type":"NUMBER","C_33_MAX":"","C_33_MIN":"0","C_38_search":"COMPRIS","C_38_type":"NUMBER","C_38_MAX":"","C_38_MIN":"0","C_36_search":"COMPRIS","C_36_type":"NUMBER","C_36_MAX":"","C_36_MIN":"","C_2038":"","C_2038_search":"EGAL","C_2038_type":"FLAG","keywords":""}',
                }

            yield FormRequest(
                "https://terre-pierre.gnimmo.com/catalog/advanced_search_result.php",
                callback=self.parse,
                formdata=formdata,
                meta={'property_type': response.meta['property_type'], "page":page+1, "max_page":max_page}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Terre_Pierre_PySpider_"+ self.country)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
            if "parking" in title.lower() or "garage" in title.lower():
                item_loader.add_value("parking", True)
            if "balcon" in title.lower():
                item_loader.add_value("balcony", True)
        
        room_count = response.xpath("//div[contains(text(),'Chambres')]/following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//div[contains(text(),'Nombre pièces')]/following-sibling::div//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
            else:
                room_count = response.xpath("substring-before(//div[@class='bulle']/span[contains(.,'pièce')]//text(),'pièce') ").get()
                if room_count:
                    item_loader.add_value("room_count",room_count)

                
        square_meters = response.xpath("//div[contains(text(),'Surface')]/following-sibling::div//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", "".join(filter(str.isnumeric, square_meters.split('m')[0].split(',')[0].split('.')[0].strip())))
        else:
            square_meters = response.xpath("//div[@class='bulle']/span[contains(.,'m²')]//text()  ").get()
            if square_meters:
                item_loader.add_value("square_meters", "".join(filter(str.isnumeric, square_meters.split('m')[0].split(',')[0].strip())))    
        address = response.xpath("//li//div[contains(.,'Ville')]/following-sibling::div//text()").get()
        if address:
            item_loader.add_value("address", address)
            # item_loader.add_value("city", address)
        else:
            address = title.split("LOUER")[-1].split("PIECE")[0].strip()
            address = address.split(" ")
            address2 = ""
            for i in address:
                if not i.isdigit():
                    address2 += i + " "
            item_loader.add_value("address", address2)
                
        zipcode = response.xpath("//span[@class='alur_location_ville']/text()").get()
        if zipcode:
            city = " ".join(zipcode.strip().split(" ")[1:])
            zipcode = zipcode.strip().split(" ")[0]
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", city)
     
        rent = response.xpath("//div[@class='container']//span[@class='alur_loyer_price']/text()").get()
        if rent:
            price  = rent.replace(" ","").replace("\xa0","").split("€")[0].split("Loyer")[1].strip()
            item_loader.add_value("rent", price.strip())

        item_loader.add_value("currency", "EUR")
        
        bathroom_count = response.xpath("//li//div[contains(.,'Salle')]/following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        else:
            bathroom_count = response.xpath("substring-before(//div[@class='bulle']/span[contains(.,'Salle de bain')]//text(),'Salle de bain') ").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count",bathroom_count)
        desc = "".join(response.xpath("//div[@class='products-description']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        images = [ x for x in response.xpath("//div[@id='slider_product_short']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        external_id = response.xpath("//span[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        floor = response.xpath("//li//div[.='Etage']/following-sibling::div//text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        utilities = response.xpath("//li//div[contains(.,'sur charges')]/following-sibling::div//text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("EUR")[0].strip())
        elif not utilities:
            utilities = response.xpath("//li//div[contains(.,'Charges forfaitaires')]/following-sibling::div//text()").get()
            if utilities:
                item_loader.add_value("utilities", utilities.split("EUR")[0].strip())
        
        deposit = response.xpath("//li//div[contains(.,'Garantie')]/following-sibling::div//text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("EUR")[0].strip())
        
        balcony = response.xpath("//li//div[contains(.,'balcon')]/following-sibling::div//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//li//div[contains(.,'Meublé')]/following-sibling::div//text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//li//div[contains(.,'Ascenseur')]/following-sibling::div//text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//li//div[contains(.,'terrasse')]/following-sibling::div//text()[.!='0']").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        swimming_pool = response.xpath("//li//div[contains(.,'Piscine')]/following-sibling::div//text()").get()
        if swimming_pool:
            if "non" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", False)
            elif "oui" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", True)
        
        energy_label = response.xpath("//li//div[contains(.,'Conso Energ')]/following-sibling::div//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        available_date = response.xpath("//li//div[contains(.,'Disponibilit')]/following-sibling::div//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        parking = response.xpath("//li//div[contains(.,'Stationnement')]/following-sibling::div//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("LatLng(")[1].split(",")[0]
            longitude = latitude_longitude.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "TERRE PIERRE GNIMMO")
        item_loader.add_value("landlord_phone", "04.90.52.08.08")
        
        yield item_loader.load_item()