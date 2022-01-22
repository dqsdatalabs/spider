import scrapy
from python_spiders.loaders import ListingLoader
import re
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
import requests
class HoppsimmobiliareSpider(scrapy.Spider):
    name = 'hoppsimmobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['hoppsimmobiliare.it']

    def start_requests(self):
        start_urls = [            
            {'url': 'https://www.hoppsimmobiliare.it/ville.asp',
                'property_type': 'house'},
            {'url': 'https://www.hoppsimmobiliare.it/appartamenti.asp',
                'property_type': 'apartment'},
            
            ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'), callback=self.parse, meta={'property_type': url.get('property_type')})
            
            
    def parse(self, response):
        for prop in response.css(".property"):
            if prop.css(".property-tag::text").get() == "Affitto":
                yield scrapy.Request(url=response.urljoin(prop.css(".property-img a::attr(href)").get()), callback=self.parse_page, meta={'property_type':response.meta.get('property_type')})
    
    def parse_page(self, response):
        item = ListingLoader(response=response)

        title = response.css(".row h3::text").get()
        rent = response.css("span[style='font-family:Arial,Helvetica,sans-serif']").xpath("//strong[contains(text(),'Richiesta')]/text()").get()
        if not rent:
            rent = response.css("span").xpath("//span[contains(text(),'Richiesta')]/text()").get()

        square_meters = response.css("span[style='font-family:Arial,Helvetica,sans-serif']").xpath("//strong[contains(text(),'Mq')]/text()").get()
        if not square_meters:
            square_meters = response.css("span[style='font-family:Arial,Helvetica,sans-serif']").xpath("//span[contains(text(),'Mq')]/text()").get()

        room_count = response.css("span[style='font-family:Arial,Helvetica,sans-serif']").xpath("//strong[contains(text(),'Vani:')]/text()").get()
        


        if not room_count:

            room_count = response.css("span[style='font-family:Arial,Helvetica,sans-serif']").xpath("//span[contains(text(),'Vani:')]/text()").get()
            if not room_count:
                room_count = response.xpath("//td[contains(text(),'Vani:')]/text()").get()

        bathroom_count = response.css("span[style='font-family:Arial,Helvetica,sans-serif']").xpath("//strong[contains(text(),'Bagni:')]/text()").get()
        if not bathroom_count:
            bathroom_count = response.css("span[style='font-family:Arial,Helvetica,sans-serif']").xpath("//span[contains(text(),'Bagni:')]/text()").get()
            if not bathroom_count:
                bathroom_count = response.xpath("//td[contains(text(),'Bagno:')]/text()").get()


        description     = remove_white_spaces(" ".join(response.css("span[style='font-family:Arial,Helvetica,sans-serif'] ::text").getall()))        

        parking = response.css("ul.bullets").xpath("//li[contains(text(),'Parking')]/text()").get()
        balcony = response.css("ul.bullets").xpath("//li[contains(text(),'Balcony')]/text()").get()
        swimming_pool = response.css("ul.bullets").xpath("//li[contains(text(),'Pool')]/text()").get()
        images = [ response.urljoin(i) for i in response.css(".fotorama img::attr(src)").getall()]

        




        terrace = ''
        if bathroom_count:
            if '+' in bathroom_count:
                bathroom_count = ''
            else:
                bathroom_count = int(bathroom_count.split(":")[1].strip())

        if room_count:
            room_count = int(room_count.split(":")[1].strip())

        if square_meters:
            if 'terrace' in square_meters:
                terrace = True
            square_meters = re.findall("[0-9]+",square_meters)[0]
            square_meters = int(square_meters)

        if rent:
            rent = re.findall('[0-9]+\.*[0-9]*',rent)[0]
            rent  = int(rent.replace(".",""))


        if swimming_pool:
            swimming_pool = True

        if balcony:
            balcony = True

        if parking:
            parking = True


        if len(description) == 0:
            description     = re.findall('DESCRIZIONE .*',remove_white_spaces(" ".join(response.css("div.tab-pane  ul li ::text").getall())))
            if description:
                description = description[0].replace("DESCRIZIONE","")
        else:
            description =  re.findall('DESCRIZIONE .*',description)
            if description:
                description = description[0].replace("DESCRIZIONE","")


        if len(description) == 0:
            description = remove_white_spaces(" ".join(response.css('span[style="color:black"] ::text').getall()))
            if description:
                description =  re.findall('DESCRIZIONE .*',description)
                description = description[0].replace("DESCRIZIONE","")
        if len(description) == 0:
            description = remove_white_spaces(" ".join(response.xpath('//span[contains(@style,"font-family:Arial,Helvetica,sans-serif")]').css(" ::text").getall()))
            if description:
                if 'DESCRIZIONE' in description:
                    description =  re.findall('DESCRIZIONE .*',description)
                    description = description[0].replace("DESCRIZIONE","")

        res = requests.get(url=response.xpath("//a[contains(@href,'https://goo.gl/maps')]/@href").get())
        latitude, longitude = re.findall(r'38\.[0-9]+,13\.[0-9]+',res.text)[0].split(",")

        responseGeocode = requests.get( f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']
        longitude = str(longitude)
        latitude = str(latitude)






        item.add_value("external_source"        ,self.external_source)
        item.add_value("external_link"          ,response.url)
        item.add_value("property_type"          ,response.meta['property_type'])
        item.add_value("title"                  ,title)
        item.add_value("rent"                   ,rent)
        item.add_value("square_meters"          ,square_meters)
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("description"            ,description)
        item.add_value("parking"                ,parking)
        item.add_value("balcony"                ,balcony)
        item.add_value("swimming_pool"          ,swimming_pool)
        item.add_value("images"                 ,images)
        item.add_value("currency"               ,"EUR")
        item.add_value("landlord_name"          ,'Hopps Immobiliare')
        item.add_value("landlord_email"         ,'info@mercatocasa.com')
        item.add_value("landlord_phone"         ,'091326502')
        item.add_value("terrace"                ,terrace)
        item.add_value("address"                ,address)
        item.add_value("zipcode"                ,zipcode)
        item.add_value("city"                   ,city)
        item.add_value("longitude"              ,longitude)
        item.add_value("latitude"               ,latitude)









        yield item.load_item()


