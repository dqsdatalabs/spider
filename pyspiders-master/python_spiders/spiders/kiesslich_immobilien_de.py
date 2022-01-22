# -*- coding: utf-8 -*-
# Author: Sriram
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class KiesslichImmobilien_Spider(scrapy.Spider):
    name = "kiesslich_immobilien"
    start_urls = ['https://www.kiesslich-immobilien.de/']
    allowed_domains = ["kiesslich-immobilien.de"]  
    country = 'germany' 
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 
    position = 1
    value=1


    def start_requests(self):       
        for url in self.start_urls:
            headers= {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9,ta;q=0.8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36" }
            yield scrapy.Request(url, headers= headers, callback=self.parse)

    def parse(self, response, **kwargs):
        id=next(iter(filter(bool,(e for e in re.findall('var\sI*guid\s*=\s\'(.*?)\';</script>',response.text,re.DOTALL)))),'')         
        js_link=f"https://homepagemodul.immowelt.de/list/api/list/?callback=listcallback&guid={id}&area=&eType=-1&eCat=-1&geoid=-1&livingarea=&page={self.value}&price=&rentfactor=&room=&squareprice=&windowarea=&stype=0"
        headers = {
            'Accept': '*/*', 
            'Content-Type': 'application/x-javascript; charset=utf-8', 
            'Connection': 'keep-alive',
            'Host': 'homepagemodul.immowelt.de', 
            'Referer': 'https://www.kiesslich-immobilien.de/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'}
      
        yield scrapy.Request(url=js_link,headers=headers,callback=self.get_url,dont_filter=True)


    def get_url(self, response,**Kwargs):
        item_links=re.findall('ToExpose\(\\\\\"(.*?)\\\\\"\)',response.text)        
        for item_link in item_links:
            item_url="https://www.kiesslich-immobilien.de/#/expose" + str(item_link)
            yield scrapy.Request(url=item_url, callback= self.get_item,dont_filter=True,meta={"item_link":item_link})
        
        next_button= next(iter(filter(bool,(e for e in re.findall(r"class=\\\"hm_btn\\\".*?ToPage\(\\\"(\d+)\\",response.text,re.DOTALL)))),'')
        if next_button:
            self.value=self.value +1
            next_page_url=f"https://www.kiesslich-immobilien.de/#/list{next_button}"
            headers= {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "en-US,en;q=0.9,ta;q=0.8",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36" }
            yield scrapy.Request(url=next_page_url,headers=headers, callback=self.parse,dont_filter=True)
        
    def get_item(self,response):
        link_got= str(response.meta["item_link"])
        
        source_url=f'https://homepagemodul.immowelt.de/home/api/Expose/?callback=exposecallback&guid=d3ebf7436eaa4b279edca3b09dd7439a&id={link_got}&isVorschau=&isStatistic=true'
        headers = {
            'Accept': '*/*', 
            'Content-Type': 'application/x-javascript; charset=utf-8', 
            'Connection': 'keep-alive',
            'Host': 'homepagemodul.immowelt.de', 
            'Referer': 'https://www.kiesslich-immobilien.de/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'}
        yield scrapy.Request(url=source_url, callback=self.populate_item,headers=headers,dont_filter=True, meta={"link_got":link_got})

    def populate_item(self, response):
        token = response.meta["link_got"]
        property_type=next(iter(filter(bool,(remove_white_spaces(e) for e in re.findall(r"Immobilienart:\\*u003c/span\\*u003e(.*?)\\*r",response.text,re.DOTALL)))),'') 
        if property_type.lower() == 'wohnung':
            property_type = "apartment"
        elif property_type.lower() == "haus":
            property_type = "house"
        elif property_type.lower() == "studentenwohnung":
            property_type = "student_apartment"
        elif property_type.lower() == "studio":
            property_type = "studio"
        else:
            property_type = ""
        purchase =next(iter(filter(bool,(e.replace(",",".") for e in re.findall(r"Kaufpreis:(.*?)€",response.text,re.DOTALL)))),'')
        if property_type != "":
            if purchase == "":
                title=next(iter(filter(bool,(remove_white_spaces(e) for e in re.findall(r'u003ch1\\*u003e(.*?)\\*r\\*n',response.text,re.DOTALL)))),'') 
                description=next(iter(filter(bool,(remove_white_spaces(e) for e in re.findall(r"Objektbeschreibung.*?\\*u003cp\\*u003e(.*?)\\*u003c/div",response.text,re.DOTALL)))),'')      
                description=self.clean_response(description)
                address=next(iter(filter(bool,(remove_white_spaces(e.replace("\\u0026#223;","b")) for e in re.findall(r"u003e\\*u003cspan\\*u003e(.*?)\\*u003c/span",response.text,re.DOTALL)))),'')            
                latitude,longitude = extract_location_from_address(address)
                zipcode, city, address = extract_location_from_coordinates(latitude,longitude)
                square_meters=next(iter(filter(bool,(e.replace(",",".") for e in re.findall(r"u003eWohnfläche:\\*u003c/span\\*u003eca.(.*?)m.\\*r",response.text,re.DOTALL)))),'') 
                if square_meters:
                    square_meters=int(float(square_meters))
                room_count=next(iter(filter(bool,(remove_white_spaces(e) for e in re.findall(r"u003eZimmer:\\*u003c/span\\*u003e(\d*?)\\*r",response.text,re.DOTALL)))),'') 
                if room_count == "":
                    room_count = 1
                parking= True if 'stellplatz' in description.lower() or  'Parken' in description.lower()  else False
                balcony= True if "balkon" in title.lower() or "balkon" in description.lower() else False
                property_type=next(iter(filter(bool,(remove_white_spaces(e) for e in re.findall(r"Immobilienart:\\*u003c/span\\*u003e(.*?)\\*r",response.text,re.DOTALL)))),'') 
                if property_type.lower() == 'mietwohnungen':
                    property_type = "apartment"
                elif property_type.lower() == 'wohnung':
                    property_type = "apartment"
                elif property_type.lower() == "haus":
                    property_type = "house"
                elif property_type.lower() == "studentenwohnung":
                    property_type = "student_apartment"
                elif property_type.lower() == "studio":
                    property_type = "studio"
                else:
                    ""
                images=re.findall(r"https://media-pics.*?\.jpg",response.text) 
                images=list(set(images))
                rent =next(iter(filter(bool,(e.replace(",",".") for e in re.findall(r"\\*u003eKaltmiete:\s*(.*?)€",response.text,)))),'')
                if rent == "":
                    rent =next(iter(filter(bool,(e.replace(",",".") for e in re.findall(r"Kaufpreis:(.*?)€",response.text,re.DOTALL)))),'')
                rent = re.sub(r'\.[\d+]*?\s*$','',rent)
                utilities =next(iter(filter(bool,(e.replace(",",".") for e in re.findall(r"Nebenkosten:\\*u003c/span\\*u003e(.*?)€\\*r\\*n",response.text,)))),'')
                if utilities:
                    utilities = re.sub(r'\.[\d+]*?\s*$','',utilities)
                deposit=next(iter(filter(bool,(e.replace(",",".") for e in re.findall(r"Kaution:\\*u003c/span\\*u003e(.*)€\\*r\\*n",response.text,re.DOTALL)))),'') 
                deposit = re.sub(r'\.[\d+]*?\s*$','',deposit)
                currency=currency_parser("€","german")
                heating_cost=next(iter(filter(bool,(e.replace(",",".") for e in re.findall(r"Warmmiete:\\*u003c/span\\*u003e(.*?)€",response.text,re.DOTALL)))),'') 
                heating_cost = re.sub(r'\.[\d+]*?\s*$','',heating_cost)
                if rent and heating_cost !='':
                    heating_cost=int(heating_cost) - int(rent)
                landlord_name ="Immobilien Harald Kießlich"
                landlord_phone ="+49 3591 491764"
                landlord_email = "info@kiesslich-immobilien.de"
                external_link = f"https://www.kiesslich-immobilien.de/#/expose{token}"

                item_loader = ListingLoader(response=response) 
                item_loader.add_value("external_link", external_link) 
                item_loader.add_value("external_source", self.external_source) 
                item_loader.add_value("position", self.position) 
                item_loader.add_value("title", title) 
                item_loader.add_value("description", description)      
                item_loader.add_value("city", city) 
                item_loader.add_value("zipcode", zipcode) 
                item_loader.add_value("address", address) 
                item_loader.add_value("latitude", str(latitude)) 
                item_loader.add_value("longitude", str(longitude)) 
                item_loader.add_value("property_type", property_type) 
                item_loader.add_value("square_meters", square_meters) 
                item_loader.add_value("room_count", convert_to_numeric(room_count))
                item_loader.add_value("parking", parking) 
                item_loader.add_value("balcony", balcony) 
                item_loader.add_value("images", images) 
                item_loader.add_value("rent", convert_to_numeric(rent)) 
                item_loader.add_value("deposit", convert_to_numeric(deposit)) 
                item_loader.add_value("utilities",convert_to_numeric(utilities))
                item_loader.add_value("currency", currency) 
                item_loader.add_value("heating_cost", convert_to_numeric(heating_cost)) 
                item_loader.add_value("landlord_name", landlord_name) 
                item_loader.add_value("landlord_phone", landlord_phone) 
                item_loader.add_value("landlord_email", landlord_email)
                self.position += 1
                yield item_loader.load_item()

    def clean_response(self,string):     
        string = string.replace("\\u003c","")
        string = string.replace("\\u003e","")
        string = string.replace("r/","")
        string = string.replace("/p","")
        string = string.replace("\\r","")
        string = string.replace("\\n","")
        string = string.replace("br/","")
        string = string.replace("/strong","")
        string = re.sub(r'\s+',' ',string)
        string = string.replace("/","")      
        return string

    

    