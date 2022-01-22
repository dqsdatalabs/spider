import scrapy
from python_spiders.loaders import ListingLoader
import re
from python_spiders.helper import remove_white_spaces
import requests

class BelivingSpider(scrapy.Spider):
    name = 'beliving'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['beliving.it']
    start_urls = ['https://beliving.it/index.php/it/affitti/la-nostra-offerta']

    def parse(self, response):
        l=[]
        for item in response.css(".row-fluid"):
            if item.css(".ospitem-watermark_types::text").get() != 'RENTED' and item.css(".ospitem-watermark_types::text").get() != 'None':
                l.append(item.css("a.ositem-hrefphoto::attr(href)").get())
        for i in set(l):
            if i == None:
                continue
            yield scrapy.Request(url=response.urljoin(i), callback=self.parse_page)

    def parse_page(self, response):
        images = response.css(".gallery_blv a img::attr(src)").getall()
        title = response.css(".propertyinfoli img::attr(title)").get()
        description = remove_white_spaces(" ".join(response.css(".tab-pane .span8 p::text").getall()))
        property_type = make_prop(response.css("div.shortinfo_blv ::text").getall()[0].split("|")[0].strip())
        square_meters =  response.css("div.shortinfo_blv ::text").re('[0-9]+\W*m$')
        room_count =  response.css("div.shortinfo_blv ::text").re('[0-9]+\W*[Vv]an')
        bathroom_count =  response.css("div.shortinfo_blv ::text").re('[0-9]+\W*[Bb]agn')
        external_id =  response.css("div.shortinfo_blv ::text").re('Ref:\W*[0-9A-Za-z]+')
        address = response.css(".propertyinfoli img::attr(title)").get().split("Affitto")[0]
        floor = response.xpath('//span[contains(text(), "Piano")]/following-sibling::span').css("::text").get()
        energy_label =  response.xpath('//span[contains(text(), "Classe energetica")]/following-sibling::span').css("::text").get()
        diswasher =  response.xpath('//li[contains(text(), "Lavastoviglie")]/text()').get()
        rent = response.css(".propertyinfoli img::attr(title)").re("€\W*[0-9]+\.*[0-9]*")
        if diswasher:
            diswasher = True
        if floor:
            floor = floor.strip()

        if energy_label:
            energy_label =  energy_label.strip()

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()
        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
        responseGeocode = requests.get( f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']
        longitude = str(longitude)
        latitude = str(latitude)

        if external_id:
            external_id= external_id[0].replace("Ref:","").strip()



        if bathroom_count:
            bathroom_count= int(bathroom_count[0].replace("Bagn","").strip())        

        if room_count:
            room_count= int(room_count[0].replace("Van","").strip())
        else:
            room_count =  response.css("div.shortinfo_blv ::text").re('[0-9]+\W*[Cc]amere')
            if room_count:
                room_count= int(room_count[0].replace("Camere","").strip())

        if square_meters:
            square_meters = int(square_meters[0].replace("m","").strip())
        balcony,dishwasher,parking,terrace , elevator = fetch_amenities([ i.strip() for i in response.css(".hvHomeInfo.span4 .zebra_blv_xf1.bold_blv::text").getall()])
        if rent and property_type != 'not_scrapable':
            rent = int(rent[0].replace("€","").replace(".","").strip())

            item = ListingLoader(response=response)
            item.add_value("external_source"            ,self.external_source)
            item.add_value("landlord_phone"             ,"(+39) 06 6830 9455")
            item.add_value("currency"                   ,"EUR")
            item.add_value("images"                     ,images)
            item.add_value("address"                    ,address) 
            item.add_value("description"                ,description)
            item.add_value("title"                      ,title)
            item.add_value("property_type"              ,property_type)
            item.add_value("external_id"                ,external_id)
            item.add_value("external_link"              ,response.url)
            item.add_value("rent"                       ,rent)
            item.add_value("room_count"                 ,room_count)
            item.add_value("bathroom_count"             ,bathroom_count)
            item.add_value("city"                       ,city)
            item.add_value("floor"                      ,floor)
            item.add_value("longitude"                  ,longitude)
            item.add_value("latitude"                   ,latitude)
            item.add_value("dishwasher"                 ,dishwasher)
            item.add_value("terrace"                    ,terrace)
            item.add_value("parking"                    ,parking)
            item.add_value("balcony"                    ,balcony)
            item.add_value("zipcode"                    ,zipcode)
            item.add_value("energy_label"               ,energy_label)
            item.add_value("elevator"                   ,elevator)
            item.add_value("square_meters"              ,square_meters)
        
            yield item.load_item()




def make_prop(val):
    val = val.lower()
    apartments  = ['apartment', 'appartamento', 'duplex','fourplex', 'attico']
    houses      = ['house', 'ville', 'villa']
    not_scrapable = ['ufficio','negozio']
    for house in houses:
        if house in val.lower():
            return 'house'
    for aprt in apartments:
        if aprt in val.lower():
            return 'apartment'

    for item in not_scrapable:
        if item in val.lower():
            return 'not_scrapable'



def fetch_amenities(l):
    balcony,dishwasher,parking,terrace , elevator = '','','','',''
    
    for i in l:
        if 'balcon' in i.lower():
            balcony = True
        elif 'dishwasher' in i.lower():
            diswasher = True
        elif 'ascensore' in i.lower():
            elevator = True
        elif 'parcheggio' in i.lower():
            parking = True
        elif 'terrazza' in i.lower():
            terrace = True

    return balcony,dishwasher,parking,terrace , elevator
