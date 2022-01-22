# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
import js2xml
import lxml.etree
import scrapy
from scrapy import Selector
from ..helper import extract_number_only, extract_rent_currency, remove_white_spaces,format_date
from ..loaders import ListingLoader
from math import ceil
from scrapy import Request,FormRequest

class Amaya64ComSpider(scrapy.Spider):
    name = 'amaya64_com'
    allowed_domains = ['www.amaya64.com']
    start_urls = ['http://www.amaya64.com']
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    thousand_separator=' '
    scale_separator=','
    position = 0
    external_source="Amaya64_PySpider_france_fr"
    # custom_settings = {"PROXY_TR_ON": True}

    def start_requests(self):     
        start_urls = [
            {
                "url" : "https://www.amaya64.com/catalog/advanced_search_result.php?action=update_search&search_id=1709594124375747&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_MAX=&keywords=&C_34_MAX=&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=",
                "property_type" : "apartment",
                "type": "1"

            },
            {
                "url" : "https://www.amaya64.com/catalog/advanced_search_result.php?action=update_search&search_id=1709591213093910&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_MAX=&keywords=&C_34_MAX=&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=",
                "property_type" : "house",
                "type": "2"

            },

        ] 
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type'),'house_type': url.get('type')}
                )

    def parse(self, response, **kwargs):
        prop = response.meta.get('property_type')
        house_type = response.meta.get('house_type')
        last_page = response.meta.get('last_page',0)
        page = response.meta.get('page', 1)
        if page == 1:
            last_page = response.xpath("//div[@id='pages-infos']/@data-per-page").get()
        seen = False
        for item in response.xpath("//div[@class='link-product']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={"property_type":prop})
            seen = True
        
        if last_page and page <int(last_page):
            page = page+1
            print(page)
            
            formdata = {
                'aa_afunc': 'call',
                'aa_sfunc': 'get_products_search_ajax',
                'aa_cfunc': 'get_scroll_products_callback',
                'aa_sfunc_args[]': '{"type_page":"carto","infinite":true,"sort":"","page":'+str(page)+',"nb_rows_per_page":9,"search_id":1709591213093910,"C_28_search":"EGAL","C_28_type":"UNIQUE","C_28":"Location","C_27_search":"EGAL","C_27_type":"TEXT","C_27":'+house_type+',"C_65_search":"CONTIENT","C_65_type":"TEXT","C_65":"","C_30_MAX":"","C_34_MIN":"","C_34_search":"COMPRIS","C_34_type":"NUMBER","C_30_MIN":"","C_30_search":"COMPRIS","C_30_type":"NUMBER","C_34_MAX":"","C_33_MAX":"","C_38_MAX":"","C_36_MIN":"","C_36_search":"COMPRIS","C_36_type":"NUMBER","C_36_MAX":"","keywords":""}'
 
            }
         
            url = f"https://www.amaya64.com/catalog/advanced_search_result.php"
            print(formdata)
            yield FormRequest(
                url, 
                formdata= formdata,
                dont_filter=True,
                callback=self.parse, meta={"page": page,"property_type":prop,"last_page":last_page,"house_type":house_type})

    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        external_id = response.url
        if external_id:
            item_loader.add_value('external_id', external_id.split("search_id=")[-1].split("&")[0])
        title = response.xpath('.//title/text()').extract_first().split(' : ')[-1]
        item_loader.add_value('title', title) 
        images=[response.urljoin(x) for x in response.xpath("//div[@id='slider_product_large']//img//@src").getall()]
        if images:
            item_loader.add_value('images', images)
        item_loader.add_xpath('description', ".//div[@class='products-description']//text()")
        rent = response.xpath("//span[@class='alur_loyer_price']/text()").get()
        if rent:
            rent=rent.replace("\xa0","").split("€")[0].strip().split(" ")[-1]
            item_loader.add_value('rent', rent)
            item_loader.add_value('currency', 'GBP')
        vendre=response.xpath("//div[.='Type de transaction']/following-sibling::div/b/text()").get()
        if vendre:
            if vendre=="A vendre":
                return 

        property_type=response.xpath("//div[.='Type de bien']/following-sibling::div/b/text()").get()
        if property_type and "Appartement" in property_type:
            item_loader.add_value("property_type","apartment")
        if "parking" in property_type.lower():
            return 
        if "local d'activité" in property_type.lower():
            return 

        square_meters = response.xpath("//div[.='Surface']/following-sibling::div/b/text()").get()
        if square_meters:
            square_meters = square_meters.split(".")[0]
            item_loader.add_value('square_meters', square_meters)
        room_count = response.xpath("//div[@class='col-sm-6'][contains(.,'pièces')]/following-sibling::div/b/text()").get()
        if room_count:
            item_loader.add_value('room_count', room_count)

        elevator = response.xpath("//div[.='Ascenseur']/following-sibling::div/b/text()").get()
        if elevator:
            
            if "oui" in elevator.lower():
                item_loader.add_value('elevator', True)
            elif "non" in elevator.lower():
                item_loader.add_value('elevator', False)
        # furnished = response.xpath('.//div[contains(text(),"Meublé")]/following::div/text()').extract_first()
        # if furnished:
        #     furnished = remove_white_spaces(furnished)
        #     if furnished == 'oui':
        #         item_loader.add_value('furnished', True)
        #     if furnished == 'non':
        #         item_loader.add_value('furnished', False)
        floor = response.xpath("//div[.='Nombre étages']/following-sibling::div/b/text()").get()
        if floor:
            item_loader.add_value('floor', floor)

        bathroom_count =response.xpath("//div[.='Salle(s) de bains']/following-sibling::div/b/text()").get()
        if bathroom_count:
            item_loader.add_value('bathroom_count', bathroom_count)
        terrace = response.xpath("//div[.='Nombre de terrasses']/following-sibling::div/b/text()").get()
        if terrace:
            item_loader.add_value("terrace",terrace)
        balcony = response.xpath("//div[.='Nombre balcons']/following-sibling::div/b/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        
        deposit = response.xpath("//div[.='Dépôt de Garantie']/following-sibling::div/b/text()").get()
        if deposit:
            item_loader.add_value('deposit', deposit.split(" ")[0])
        
        energy_label = response.xpath("//div[.='Consommation énergie primaire']/following-sibling::div/b/text()").get()
        if not "vierge" in energy_label.lower():
            item_loader.add_value('energy_label',energy_label)
        
        parking = response.xpath("//div[.='Nombre places parking']/following-sibling::div/b/text()").get()
        if parking: 
            item_loader.add_value("parking",True)

        
        utilities = response.xpath("//div[.='Provision sur charges']/following-sibling::div/b/text()").get()
        if utilities:
            item_loader.add_value('utilities', utilities.split(" ")[0])
        
        city = response.xpath("//div[.='Ville']/following-sibling::div/b/text()").get()
        if city:
            item_loader.add_value('city', city)
        zipcode = response.xpath("//div[.='Code postal']/following-sibling::div/b/text()").get()
        if zipcode:
            item_loader.add_value('zipcode', zipcode)
        if city and zipcode:
            item_loader.add_value('address', city + ', ' + zipcode)


        lat=response.xpath("//script[contains(.,'google.maps.LatLng')]/text()").get()
        if lat:
            lat=lat.split("myOptions")[0].split("LatLng")[-1].split(")")[0].split(",")[0].replace("(","")
            item_loader.add_value("latitude",lat)
        lng=response.xpath("//script[contains(.,'google.maps.LatLng')]/text()").get()
        if lng:
            lng=lng.split("myOptions")[0].split("LatLng")[-1].split(")")[0].split(",")[-1].replace("-","").strip()
            item_loader.add_value("longitude",lng)

        # available = response.xpath('.//p[@class="dt-dispo"]/text()').extract_first()
        # if available:
        #     available_check = re.findall(r'\d{2}\/\d{2}\/\d{4}', available)
        #     if available_check:
        #         available_date = available_check[0]
        #         item_loader.add_value('available_date', format_date(available_date, date_format='%d/%m/%Y'))
        
                
        item_loader.add_value('landlord_name','Amaya64')
        item_loader.add_value('landlord_phone', '+33559828993')
        item_loader.add_value('landlord_email', 'immobilier@amaya64.fr')
        self.position+=1
        item_loader.add_value('position',self.position)
        item_loader.add_value("external_source", self.external_source)
        yield item_loader.load_item()