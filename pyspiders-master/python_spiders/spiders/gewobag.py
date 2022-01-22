import scrapy
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
from python_spiders.loaders import ListingLoader
import requests
class GewobagSpider(scrapy.Spider):
    name = 'gewobag'
    execution_type = 'testing'
    country = 'germany'
    locale = 'de'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['gewobag.de']
    start_urls = ['https://www.gewobag.de/fuer-mieter-und-mietinteressenten/mietangebote/?bezirke_all=&bezirke%5B%5D=charlottenburg-wilmersdorf&bezirke%5B%5D=charlottenburg-wilmersdorf-charlottenburg&bezirke%5B%5D=charlottenburg-wilmersdorf-nord&bezirke%5B%5D=charlottenburg-wilmersdorf-grunewald&bezirke%5B%5D=charlottenburg-wilmersdorf-wilmersdorf&bezirke%5B%5D=friedrichshain-kreuzberg&bezirke%5B%5D=friedrichshain-kreuzberg-friedrichshain&bezirke%5B%5D=friedrichshain-kreuzberg-kreuzberg&bezirke%5B%5D=lichtenberg&bezirke%5B%5D=lichtenberg-alt-hohenschoenhausen&bezirke%5B%5D=lichtenberg-falkenberg&bezirke%5B%5D=lichtenberg-fennpfuhl&bezirke%5B%5D=lichtenberg-friedrichsfelde&bezirke%5B%5D=marzahn-hellersdorf&bezirke%5B%5D=marzahn-hellersdorf-marzahn&bezirke%5B%5D=mitte&bezirke%5B%5D=mitte-gesundbrunnen&bezirke%5B%5D=mitte-tiergarten&bezirke%5B%5D=neukoelln&bezirke%5B%5D=neukoelln-britz&bezirke%5B%5D=neukoelln-buckow&bezirke%5B%5D=neukoelln-rudow&bezirke%5B%5D=pankow&bezirke%5B%5D=pankow-prenzlauer-berg&bezirke%5B%5D=reinickendorf&bezirke%5B%5D=reinickendorf-hermsdorf&bezirke%5B%5D=reinickendorf-tegel&bezirke%5B%5D=reinickendorf-waidmannslust&bezirke%5B%5D=spandau&bezirke%5B%5D=spandau-haselhorst&bezirke%5B%5D=spandau-staaken&bezirke%5B%5D=spandau-wilhelmstadt&bezirke%5B%5D=steglitz-zehlendorf&bezirke%5B%5D=steglitz-zehlendorf-lichterfelde&bezirke%5B%5D=steglitz-zehlendorf-wannsee&bezirke%5B%5D=tempelhof-schoeneberg&bezirke%5B%5D=tempelhof-schoeneberg-mariendorf&bezirke%5B%5D=tempelhof-schoeneberg-marienfelde&bezirke%5B%5D=tempelhof-schoeneberg-schoeneberg&bezirke%5B%5D=treptow-koepenick&bezirke%5B%5D=treptow-koepenick-alt-treptow&nutzungsarten%5B%5D=wohnung&sort-by=recent&gesamtmiete_von=&gesamtmiete_bis=&gesamtflaeche_von=&gesamtflaeche_bis=&zimmer_von=&zimmer_bis=']

    def parse(self, response):
        for url in response.css("div.angebot-footer .read-more-link::attr(href)").getall():
            yield scrapy.Request(url=url, callback=self.parse_page)

    def parse_page(self, response):
        title = response.css("h1.entry-title::text").get()
        external_id = response.xpath('//span[contains(text(), "Wohnungs Nr.")]/text()').get().split(":")[1].strip()
        rent =round(float(response.xpath('//div[contains(text(), "Gesamtmiete")]/following-sibling::div/text()').get().replace("Euro","").replace(".","").replace(",",".").strip()))
        utilities = round(float(response.xpath('//div[contains(text(), "Gesamtmiete")]/following-sibling::div/text()').get().replace(".","").replace("Euro","").replace(",",".").strip())) - rent
        deposit = round(float(response.xpath('//div[contains(text(), "Kaution")]/following-sibling::div/text()').get().replace("€","").replace(",",".").strip()))
        # address = response.xpath('//div[contains(text(), "Anschrift")]/following-sibling::div/text()').get().strip()
        latitude = response.css("input::attr(data-lat)").get()
        longitude = response.css("input::attr(data-lng)").get()
        floor = response.xpath('//div[contains(text(), "Etage")]/following-sibling::div/text()').get().strip()
        room_count = round(float(response.xpath('//div[contains(text(), "Anzahl Zimmer")]/following-sibling::div/text()').get().strip()))
        square_meters = round(float(response.xpath('//div[contains(text(), "Fläche in m²")]/following-sibling::div/text()').get().replace("m²","").replace(",",".").strip()))
        available_date = "-".join(response.xpath('//div[contains(text(), "Frei ab")]/following-sibling::div/text()').get().strip().split(".")[::-1])
        energy_label = response.xpath('//div[contains(text(), "Energieeffizienzklasse")]/following-sibling::div/text()').get()
        description = remove_white_spaces(response.xpath('//h3[contains(text(), "Objektbeschreibung")]/following-sibling::p/text()').get().strip())
        balcony, terrace, elevator, pets_allowed = search_in_desc("|-|".join(response.css(".details-characteristics ul li::text").getall()).lower())
        landlord_name = response.xpath('//strong[contains(text(), "Ansprechpartner")]/following-sibling::p/text()').get().strip()
        landlord_phone = response.xpath('//p[contains(text(), "Fon")]/text()').get()
        landlord_email = "service@gewobag.de"
        floor_plan_images = response.css(".media-plan img::attr(src)").getall()
        if landlord_phone:
            landlord_phone = landlord_phone.split(":")[1].strip()
        images = []
        for i in  response.css(".element-media img::attr(src)").getall()[:-2]:
            if '.jpg.jpg' not in i :
                images.append(i)


        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        item = ListingLoader(response=response)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("external_id"            ,external_id)
        item.add_value("terrace"                ,terrace)
        item.add_value("pets_allowed"           ,pets_allowed)
        item.add_value("address"                ,address)
        item.add_value("available_date"         ,available_date)
        item.add_value("external_link"          ,response.url)
        item.add_value("currency"               ,"EUR")
        item.add_value("images"                 ,images)
        item.add_value("title"                  ,title)
        item.add_value("rent"                   ,rent)
        item.add_value("square_meters"          ,square_meters)
        item.add_value("room_count"             ,room_count)
        # item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("property_type"          ,'apartment')
        # item.add_value("parking"                ,parking)
        item.add_value("description"            ,description)
        item.add_value("landlord_phone"         ,landlord_phone)
        item.add_value("landlord_name"          ,landlord_name)
        item.add_value("landlord_email"         ,landlord_email)
        item.add_value("energy_label"           ,energy_label)
        item.add_value("balcony"                ,balcony)
        item.add_value("floor"                  ,floor)
        item.add_value("elevator"               ,elevator)
        item.add_value("zipcode"                ,zipcode)
        item.add_value("city"                   ,city)
        item.add_value("deposit"                ,deposit)
        item.add_value("utilities"              ,utilities)
        item.add_value("latitude"               ,latitude)
        item.add_value("longitude"              ,longitude)
        item.add_value("floor_plan_images"      ,floor_plan_images)


        yield item.load_item()

def search_in_desc(desc):
    balcony, terrace, elevator,pets_allowed = '', '', '',''
    if 'terasse' in desc:
        terrace = True

    if 'balkon' in desc:
        balcony = True
    if 'fahrstuhl' in desc:
        elevator = True

    if 'haustiere erlaubt' in desc:
        pets_allowed = True



    return balcony, terrace, elevator, pets_allowed
