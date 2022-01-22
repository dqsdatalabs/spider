# -*- coding: utf-8 -*-
# Author: Aishwarya
import scrapy
from ..loaders import ListingLoader
import re
from ..helper import remove_white_spaces,remove_unicode_char,currency_parser,extract_location_from_address,strip_tags,extract_location_from_coordinates


class JeImmobilien_Spider(scrapy.Spider):
    name = "je_immobilien"
    start_urls = ['http://www.je-immobilien.de/']
    allowed_domains = ["je-immobilien.de"]
    country = 'Germany' 
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        start_urls = [{'url': 'http://www.je-immobilien.de/index.php?page=onlineshop&shop=k&k=1'}]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'), callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for block in response.xpath('//li[@class="sw_ols_artikel_uebersicht"]'):
            links = block.xpath('.//h2/a/@href').get('')
            if links != '':
                link = response.urljoin(links)
                yield scrapy.Request(link,callback=self.populate_item)

        next_pages = response.xpath('//a[contains(text(),"nächste Seite")]/@href').get('')
        if next_pages != '':
            next_page = response.urljoin(next_pages)
            yield scrapy.Request(next_page,callback=self.parse)
            
    # 3. SCRAPING level 3
    def populate_item(self, response):
        rent_check = remove_white_spaces(response.xpath('//a[@class="menu_a_2"]/text()').get(''))
        rate_block = response.xpath('//strong[contains(text(),"Wichtiges im Überblick")]/parent::p/following-sibling::p').get('')
        if (rent_check != 'Gewerbeobjekte') and (rate_block != ''): # To remove the commercial rental and non rental properties
            title = response.xpath('//h1/text()').get('')
            external_id = remove_unicode_char(response.xpath('//span[@class="e_k"]/text()').get(''))
            if re.search('Objekt\-ID\s*:\s*',external_id):
                external_id = re.sub('Objekt\-ID\s*:\s*','',external_id)
            rent = None
            currency = None
            deposit = None
            heating_cost = None
            square_meters = None
            room_count = None
            floor = None
            if re.search('Kaltmiete:\s*([\w\W]*?)\,',rate_block,re.DOTALL):
                rent = re.findall('Kaltmiete:\s*([\w\W]*?)\,',rate_block,re.DOTALL)[0]
                currency = currency_parser("€","german")
            if re.search('Kaution:\s*\d*\s*\w*\d*\s*\(([\w\W]*?)\,\-',rate_block,re.DOTALL):
                deposit = re.findall('Kaution:\s*\d*\s*\w*\d*\s*\(([\w\W]*?)\,\-',rate_block,re.DOTALL)[0]
            if re.search('Nebenkosten\s*\:\s*([\w\W]*?)\,\-',rate_block,re.DOTALL):
                heating_cost = re.findall('Nebenkosten\s*\:\s*([\w\W]*?)\,\-',rate_block,re.DOTALL)[0]
            if re.search('Wohnfläche\:\s*ca\.\s*([\w\W]*?)\s*m²',rate_block,re.DOTALL):
                square_meters = re.findall('Wohnfläche\:\s*ca\.\s*([\w\W]*?)\s*m²',rate_block,re.DOTALL)[0]
            if re.search('Zimmer\:\s*([\w\W]*?)\s*<',rate_block,re.DOTALL):
                room_count = re.findall('Zimmer\:\s*([\w\W]*?)\s*<',rate_block,re.DOTALL)[0]
                if room_count == '':
                    room_count = '1'
            if re.search('Geschoss\:\s*(\d*?)\.\s*',rate_block,re.DOTALL):
                floor = re.findall('Geschoss\:\s*(\d*?)\.\s*',rate_block,re.DOTALL)[0]
            else:
                floor = re.findall('Geschoss\:\s*([\w\W]*?)\s*<',rate_block,re.DOTALL)[0]
                if floor == 'DG':
                    floor = 'top_floor'
                elif floor == 'EG':
                    floor = 'ground_floor'
                else:
                    floor = re.findall('\d+',rate_block,re.DOTALL)[0]
            address_block = remove_white_spaces(response.xpath('//strong[contains(text(),"Lagebeschreibung")]/following-sibling::div/p/text()').get(''))
            if address_block == '':
                address_block = remove_white_spaces(response.xpath('//strong[contains(text(),"Lagebeschreibung")]/following-sibling::p/text()').get(''))
            
            city = None
            zipcode = None
            address = None
            if len(address_block.split('-')) == 2:
                addresses = address_block.split('-')
                detail = addresses[0]
            elif len(address_block.split(' ')) == 2:
                addresses = address_block.split('-')
                detail = addresses[0]
            elif len(address_block.split(' ')) > 2:
                detail = re.findall('von\s*([\w\W]*?)\s',address_block,re.I)[0]
                address_block = detail
            latitude = None
            longitude = None
            try:
                longitude,latitude = extract_location_from_address(address_block)
            except:
                pass
            zipcode, city, address = extract_location_from_coordinates(longitude,latitude)
            description_block = response.xpath('//div[contains(text(),"Allgemein")]/parent::div').get('')
            description = None
            try:
                description = strip_tags(re.findall('<strong>Ausstattung &amp; Merkmale<\/strong>([\w\W]*?)<strong>Lagebeschreibung<\/strong>',description_block,re.DOTALL)[0])
            except:
                description_block = response.xpath('//strong[contains(text(),"Wohnungssuche & Abwicklung")]/parent::p/following-sibling::p').get('')
                description = strip_tags(re.findall('<p>\s*([\w\W]*?)\s*<a\s*title',description_block,re.DOTALL)[0])
            image = response.xpath('//div[@style="padding-bottom:10px;"]/img/@src').getall()
            if image == []:
                image = response.xpath('//div[@id="sw_ols_artikel_bild"]//img/@src').getall()
            images =['http://www.je-immobilien.de/'+ x for x in image ]         
            terrace = None
            if re.search('.*(Terrasse).*',description,re.I):
                terrace = True
            else:
                terrace = False
            parking = None
            if re.search('.*(Stellplatz).*',description,re.I):
                parking = True
            else:
                parking = False
            property_type = None
            if 'Wohnung' in rent_check:
                property_type = 'apartment'
            elif 'Häuser' in rent_check:
                property_type = 'house'
            land_block = response.xpath('//div[@id="bottom"]').get('')
            landlord_name = remove_white_spaces(remove_unicode_char(re.findall('<img src="pix\/kws2011_2_rss\.gif" border="0" hspace="5" vspace="2" align="middle">([\w\W]*?)\s*\|',land_block,re.I)[0]))
            landlord_name = landlord_name.replace('//-->','').strip()
            landlord_phone = remove_white_spaces(re.findall('Telefon\:\s*([\w\W]*?)\s*\|',land_block,re.DOTALL)[0])
            landlord_email = re.findall('<a href\=\"index\.php\?page=kontakt\">([\w\W]*?)<\/a>',land_block,re.DOTALL)
          
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source) 
            item_loader.add_value("external_id", external_id) 
            item_loader.add_value("position", self.position) 
            item_loader.add_value("title", title) 
            item_loader.add_value("description", description) 
            item_loader.add_value("city", city) 
            item_loader.add_value("address", address) 
            item_loader.add_value("zipcode", zipcode) 
            item_loader.add_value("latitude", str(latitude)) 
            item_loader.add_value("longitude", str(longitude)) 
            item_loader.add_value("floor", floor) 
            item_loader.add_value("property_type", property_type) 
            item_loader.add_value("square_meters", square_meters) 
            item_loader.add_value("room_count", room_count) 
            item_loader.add_value("parking", parking) 
            item_loader.add_value("terrace", terrace) 
            item_loader.add_value("images", images) 
            item_loader.add_value("external_images_count", len(images)) 
            item_loader.add_value("rent", rent) 
            item_loader.add_value("deposit", deposit) 
            item_loader.add_value("currency", currency) 
            item_loader.add_value("heating_cost", heating_cost) 
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_phone", landlord_phone) 
            item_loader.add_value("landlord_email", landlord_email) 
            
            self.position += 1
            yield item_loader.load_item()